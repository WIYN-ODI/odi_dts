#!/usr/bin/env python3

import pika
import config
import os
import sys
import json
import datetime
import logging

logging.getLogger("pika").setLevel(logging.WARNING)

class PPA (object):

    def __init__(self, server_name=None, username=None, password=None, virtual_host=None, port=5672, reuse=True):

        self.server_name = server_name if server_name is not None else config.ppa_server_name
        self.username = username if username is not None else config.ppa_username
        self.password = password if password is not None else config.ppa_password
        self.portnumber = port if port is not None else config.ppa_port
        self.virtual_host = virtual_host if virtual_host is not None else config.ppa_virtualhost

        self.credentials = pika.PlainCredentials(
            username=self.username, password=self.password,
            erase_on_connect=False,
        )
        self.parameters = pika.ConnectionParameters(
            host=self.server_name,
            port=self.portnumber,
            virtual_host=self.virtual_host,
            credentials=self.credentials,
        )
        self.connection = None

        # if (reuse):
        #     self.channel = None
        #     self.queue = None

        self.connect()

    def connect(self):
        if (self.connection is None):
            try:
                self.connection = pika.adapters.BlockingConnection(
                    parameters=self.parameters,
                )
            except pika.exceptions.ConnectionClosed:
                print("Unable to connect to PPA AMQP server")

            # self.connection = pika.adapters.select_connection.SelectConnection(
            #     parameters=self.parameters,
            # )

            self.channel = self.connection.channel()
            self.channel.confirm_delivery()

        return (self.connection is not None and self.channel is not None)



    def _on_connect(self):
        return


    def send_message(self, target_queue, message):
        if (not self.connect()):
            return False

        queue = self.channel.queue_declare(queue=target_queue, durable=True)

        success = self.channel.basic_publish(
            exchange='', #target_queue, #"odi",
            routing_key=target_queue,
            body=message,
            properties=pika.BasicProperties(content_type='text/plain', delivery_mode=1)
        )
        # print(success)
        return success

        # channel.basic_publish()


    def report_exposure(self, timestamp, obsid, msg_type):

        msg = json.dumps({
            'time': str(timestamp),
            'id': obsid,
            'source': 'instrument',
            'destination': 'ppa',
            'type': msg_type,
        })

        return self.send_message("instrument.ralftest", msg)


if __name__ == "__main__":

    ppa = PPA()

    msg = json.dumps({
        'time': str(datetime.datetime.now()),
        'id': 'o20180111T123456.1',
        'source': 'instrument',
        'destination': 'ppa',
        'type': 'test_debug',
    })
    print(msg)

    ppa.send_message(target_queue="instrument.ralftest",
                     message=msg)

