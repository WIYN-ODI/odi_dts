#!/usr/bin/env python
import logging
import pika
import sys
import time

LOGGER = logging.getLogger(__name__)
#https://pika.readthedocs.org/en/latest/blocking_channel_use.html#pika.adapters.blocking_connection.BlockingChannel

class Listener(object):
    """
    Contains ODI listener base class that can be used to implement various
    listeners that can withstand various error conditions.
    Developed at PTI@IU and used here with permission.
    """

    def __init__(self, params):
        LOGGER.info('Listener ctor')
        self._connection = None
        self._channel = None
        self._params = params

        self._closing = False

    def connect(self):
        LOGGER.info('Connecting to %s', self._params)
        self._connection = pika.SelectConnection(self._params, self.on_connection_open, stop_ioloop_on_close=False)
        self._connection.ioloop.start()

        #this no longer works on pika 0.9.13.. (and I can't get pika.AsyncoreConnection to work)
        #self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_open(self, unused_connection):
        LOGGER.info('Connection opened')
        self._connection.add_on_close_callback(self.on_connection_closed)
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        LOGGER.info('Channel opened')
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)

        self.on_connected(); #finally call user func.

    def on_channel_closed(self, channel, reply_code, reply_text):
        LOGGER.warning('Channel %i was closed: (%s) %s',
                       channel, reply_code, reply_text)
        self._connection.close()

    def on_connected(self):
        LOGGER.info('on_connected is not overridden')

    def close_connection(self):
        LOGGER.info('Closing connection')
        self._connection.close()

    def on_connection_closed(self, connection, reply_code, reply_text):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning('Connection closed, reopening in 5 seconds: (%s) %s',
                           reply_code, reply_text)
            self._connection.add_timeout(5, self.reconnect)

    def reconnect(self):
        # This is the old connection IOLoop instance, stop its ioloop
        self._connection.ioloop.stop()

        if not self._closing:
            # Create a new connection
            self._connection = self.connect()

            # There is now a new connection, needs a new ioloop to run
            self._connection.ioloop.start()

    def on_channel_closed(self, method_frame):
        LOGGER.warning('Channel was unexpectedly closed - probably due violation of exchange / queue protocol')
        self._connection.close()

    def on_cancelok(self, unused_frame):
        LOGGER.info('RabbitMQ acknowledged the cancellation of the consumer')
        self._channel.close()

    def stop(self):
        LOGGER.info('Stopping')
        self._closing = True
        if self._channel:
            LOGGER.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)
        self._connection.ioloop.stop()
        LOGGER.info('Stopped')
        #self._channel.close()
        #self.close_connection()


