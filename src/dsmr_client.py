import argparse
import asyncio

import json
import logging
import os

from async_timeout import timeout

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
        handled = 0

        try:
            LOGGER.info("connecting to dsmr reader")
            reader, writer = await asyncio.open_connection(self.host, self.port,
                                                           loop=loop)

            while True:
                with timeout(30):
                    data = await reader.readline()
                    self.telegram_buffer.append(data.decode('ascii'))

                    for raw_telegram in self.telegram_buffer.get_all():
                        handled += 1
                        try:
                            telegram = self.telegram_parser.parse(raw_telegram)
                            # Push newly parsed telegram onto queue
                            await queue.put(telegram)
                        except ParseError as e:
                            LOGGER.warning('Failed to parse telegram: %s', e)
        except asyncio.QueueFull as e:
            LOGGER.error('Queue full')
        except Exception as e:
            LOGGER.error(e)
        finally:
            try:
                writer.close()
            except:
                pass
            try:
                reader.close()
            except:
                pass

        return handled


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
        LOGGER.info("connecting to mqtt broker")

        published = 0
        try:
            await C.connect('mqtt://{host}:{port}/'.format(host=self.host,
                                                           port=self.port))

            LOGGER.info("mqtt connected")

            while True:
                telegram = await self.queue.get()
                body = self.format_telegram(telegram)
                
                await C.publish('sensors/dsmr', body.encode('utf8'))
                published += 1
        except Exception as e:
            LOGGER.error("Error in connect/publish loop")
        finally:
            try:
                yield from C.disconnect()
            except:
                pass

        return published 


async def main(loop, args):
    mqtt = MQTTTransport(args.mqtt_host, args.mqtt_port)
    tcp_reader = TcpReader(args.dsmr_host, args.dsmr_port, telegram_specifications.V4)

    LOGGER.info("pre-start")

    mqtt_f = asyncio.ensure_future(mqtt.run())
    tcp_reader_f = asyncio.ensure_future(tcp_reader.read(mqtt.queue, loop))


    done, pending = await asyncio.wait([mqtt_f, tcp_reader_f],
                                       return_when=asyncio.FIRST_COMPLETED)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='DSMR P1 port over TCP <-> MQTT bridge')

    parser.add_argument('--mqtt_host', type=str,
                        default=os.environ.get('MQTT_HOST', '172.16.0.2'))
    parser.add_argument('--mqtt_port', type=int,
                        default=os.environ.get('MQTT_PORT', 1883))

    parser.add_argument('--dsmr_host', type=str,
                        default=os.environ.get('DSMR_HOST', '192.168.2.2'))
    parser.add_argument('--dsmr_port', type=int,
                        default=os.environ.get('DSMR_PORT', 23))

    parser.add_argument('--verbose', action='store_true')

    args = parser.parse_args()

    logging.basicConfig()
    level = logging.INFO

    if args.verbose:
        level = logging.DEBUG

    logging.getLogger().setLevel(level)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop, args))
    loop.close()

