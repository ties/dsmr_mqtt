import argparse
import asyncio

import json
import logging
import os

from dsmr_parser import telegram_specifications, obis_references
from dsmr_parser.clients.telegram_buffer import TelegramBuffer
from dsmr_parser.exceptions import ParseError
from dsmr_parser.parsers import TelegramParser

from hbmqtt.client import MQTTClient

LOGGER = logging.getLogger(__name__)


class TcpReader(object):
    def __init__(self, host, port, telegram_specification):
        self.host = host
        self.port = port

        self.telegram_parser = TelegramParser(telegram_specification)
        self.telegram_buffer = TelegramBuffer()

    async def read(self, queue, loop):
        reader, writer = await asyncio.open_connection(self.host, self.port,
                                                       loop=loop)

        while True:
            data = await reader.readline()
            self.telegram_buffer.append(data.decode('ascii'))

            for raw_telegram in self.telegram_buffer.get_all():
                try:
                    telegram = self.telegram_parser.parse(raw_telegram)
                    # Push newly parsed telegram onto queue
                    await queue.put(telegram)
                except asyncio.QueueFull as e:
                    LOGGER.error('Queue full')
                    raise e
                except ParseError as e:
                    LOGGER.warning('Failed to parse telegram: %s', e)
        
        writer.close()
        reader.close()

class MessagePrinter(object):
    def __init__(self):
        self.queue = asyncio.Queue()

    async def run(self):
        inverse_references = {v: k for (k,v) in
                              obis_references.__dict__.items()
                              if isinstance(v, str) }

        while True:
            telegram = await self.queue.get()

            self.queue.task_done()
            # message_datetime = telegram[obis_references.P1_MESSAGE_TIMESTAMP]
            for ref, value in telegram.items():
                print(inverse_references[ref], value.unit, value.value)

class MQTTTransport(object):
    def __init__(self, host, port):
        self.queue = asyncio.Queue()

        self.host = host
        self.port = port

    def format_telegram(self, telegram):
        body = {
            'tariff1': float(telegram[obis_references.ELECTRICITY_USED_TARIFF_1].value),
            'tariff2': float(telegram[obis_references.ELECTRICITY_USED_TARIFF_2].value),
            'active_tariff': int(telegram[obis_references.ELECTRICITY_ACTIVE_TARIFF].value),
            'current_usage': int(1000 * telegram[obis_references.CURRENT_ELECTRICITY_USAGE].value),
            'current_power_l1': int(1000 * telegram[obis_references.INSTANTANEOUS_ACTIVE_POWER_L1_POSITIVE].value),
            'current_power_l2': int(1000 * telegram[obis_references.INSTANTANEOUS_ACTIVE_POWER_L2_POSITIVE].value),
            'current_power_l3': int(1000 * telegram[obis_references.INSTANTANEOUS_ACTIVE_POWER_L3_POSITIVE].value),
        }

        return json.dumps(body)

    async def run(self):
        C = MQTTClient()
        await C.connect('mqtt://{host}:{port}/'.format(host=self.host,
                                                       port=self.port))

        LOGGER.info("Starting reader loop")

        while True:
            telegram = await self.queue.get()
            body = self.format_telegram(telegram)
            
            await C.publish('sensors/dsmr', body.encode('utf8'))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='DSMR P1 port over TCP <-> MQTT bridge')

    parser.add_argument('--mqtt_host', type=str,
                        default=os.environ.get('MQTT_HOST', '172.16.0.2'))
    parser.add_argument('--mqtt_port', type=int,
                        default=os.environ.get('MQTT_PORT', 1883))

    parser.add_argument('--dsmr_host', type=str,
                        default=os.environ.get('DSMR_HOST', '192.168.2.7'))
    parser.add_argument('--dsmr_port', type=int,
                        default=os.environ.get('DSMR_PORT', 23))

    parser.add_argument('--verbose', action='store_true')

    args = parser.parse_args()

    logging.basicConfig()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    loop = asyncio.get_event_loop()
    mqtt = MQTTTransport(args.mqtt_host, args.mqtt_port)
    tcp_reader = TcpReader(args.dsmr_host, args.dsmr_port, telegram_specifications.V4)

    asyncio.ensure_future(mqtt.run(), loop=loop)
    loop.run_until_complete(tcp_reader.read(mqtt.queue, loop))
    loop.close()

