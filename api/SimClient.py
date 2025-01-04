import socket
import nsb_payload as nsbp
import struct
import logging
import random
import os
import asyncio
from aioconsole import ainput
import uuid
import time
import json
import functools
import threading

# Rabbit/Pika
import pika
import pika.exceptions

logging.basicConfig(level=logging.FATAL, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',)
# Create a server logger
slog = logging.getLogger(f"(server)")
slog.setLevel(level=logging.INFO)
# Create a rabbit logger
rlog = logging.getLogger(f"(rabbit client)")
rlog.setLevel(logging.INFO)

# NSB_SERVER_ADDR = "host.docker.internal"
NSB_SERVER_ADDR = nsbp.HOST
RABBIT_URL = 'amqp://guest:guest@localhost:5672/%2F'

"""
HEADER:
Length (4 Bytes)
Source ID (4 Bytes)
Destination ID (4 Bytes)
Message Type (1 Byte)
"""

# The class responsible for managing Rabbit-related stuff
class SimRabbitManager:
    def __init__(self, sim_name, callback):
        
        self.sim_name = sim_name
        
        self._connection = None
        self._channel = None
        self._rabbiturl = RABBIT_URL
        self._stopping = False
        self._closing_connection = False
        self._MAINEXCHANGE = "main_router"
        self._callback = callback
        
        self._txq = None
        self._txq_name = "global_txq" # Any node client or sim client can create the txq, only SimClient can close txq
        self._txq_tag = None
        
        
    def start(self):
        rlog.info(f"Starting RabbitMQ for sim {self.sim_name}")
        self._stopping = False
        self._closing_connection = False

        try:
            rlog.info("Attempting to establish connection...")
            self._connection = pika.SelectConnection(
                pika.URLParameters(self._rabbiturl),
                on_open_callback=self.on_connection_open,
                on_open_error_callback=self.on_connection_open_error,
                on_close_callback=self.on_connection_closed
            )
            rlog.info("Sim client entering Rabbit IOLoop.")
            self._connection.ioloop.start()
        except KeyboardInterrupt:
            rlog.warning("KeyboardInterrupt detected, stopping RabbitManager.")
            self.stop()
        finally:
            self.stop()
        
    def stop(self):
        """Gracefully stop the RabbitMQ connection"""
        rlog.info(f"Stopping sim {self.sim_name}")
        self._stopping = True

        # Close the channel if it's open
        if self._channel and self._channel.is_open:
            try:
                cb = functools.partial(self.on_cancelok, queue_name=self._txq)
                self._channel.basic_cancel(self._txq_tag, cb)
            except pika.exceptions.ChannelWrongStateError:
                rlog.warning("Channel already closing, skipping cancel.")
            self._channel.close()

        # Close the connection only if it is open and not already closing
        if self._connection and not self._connection.is_closed and not self._connection.is_closing:
            rlog.info("Closing the RabbitMQ connection.")
            self._closing_connection = True
            self._connection.close()
            self._connection.ioloop.stop()
        else:
            rlog.warning("Connection already closing or closed.")

            
    def on_connection_open(self, connection_obj):
        rlog.info(f"Connection was established for sim {self.sim_name}")
        self.open_channel()
        
    def on_connection_open_error(self,  _unused_connection, err):
        rlog.error(f"Sim {self.sim_name} failed while opening connection: ", err)
        
    def on_connection_closed(self, _unused_connection, reason):
        """
        This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.
        """
        rlog.warning(f"Sim{self.sim_name}'s connection closed unexpectedly: {reason}")
        if self._stopping:
            self.stop()
            
        
    def open_channel(self):
        # Create our channel to handle all messaging
        self._connection.channel(on_open_callback=self.on_channel_open)
    
    def on_channel_open(self, channel):
        rlog.info(f"Sim {self.sim_name}'s channel was successfully opened")
        # Store the channel object in self._channel
        self._channel = channel
        # Add a channel closed callback to the channel
        self._channel.add_on_close_callback(self.on_channel_closed)
        # Set up the main exchange with some name
        self.setup_exchange()
        
    def on_channel_closed(self, channel, reason):
        """
        Invoked by pika when RabbitMQ unexpectedly closes the channel.
        """
        rlog.warning(f"Sim {self.sim_name}'s channel {channel} was closed unexpectedly: {reason}")
        self._channel = None
         
    def setup_exchange(self):
        rlog.info(f"Setting up exchange {self._MAINEXCHANGE} if not already set up")
        cb = functools.partial(self.on_exchange_declareok, exchange_name=self._MAINEXCHANGE)
        self._channel.exchange_declare(exchange=self._MAINEXCHANGE, exchange_type="direct", callback=cb)
        
    # When the exchange was correctly declared (or already exists)
    def on_exchange_declareok(self, _unused_frame, exchange_name):
        """
        Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command. 
        """
        rlog.info(f"Exchange declared: {exchange_name}")
        self.setup_global_txq()  # Set up the global TX queue

    def setup_global_txq(self):
        """
        Declares the global TX queue and binds it to the exchange.
        """
        rlog.info(f"Setting up global TX queue: {self._txq_name}")
        cb = functools.partial(self.on_queue_declareok, queue_name=self._txq_name)
        self._channel.queue_declare(queue=self._txq_name, exclusive=False, callback=cb)
        
        
    # Bind a queue to the channel (queue_name will be queue-specific, as defined in functools.partial above)
    def on_queue_declareok(self, method_frame, queue_name):
        """
        Handles queue declaration and binds the queue to the exchange.
        """
        rlog.info(f"Queue declared: {queue_name}")
        # Bind RX queue and start consuming
        cb = functools.partial(self.on_bindok, queue=method_frame.method.queue, queue_name=queue_name)
        self._channel.queue_bind(queue=method_frame.method.queue, exchange=self._MAINEXCHANGE, callback=cb)
        
    # If the bind succeeded, log the success
    def on_bindok(self, _unused_frame, queue, queue_name):
        
        rlog.info(f"Successfully bound queue {queue_name}/{queue} to exchange {self._MAINEXCHANGE}!")
        self.start_consuming(queue=queue, queue_name=queue_name)
        
    # The function to start consuming from our queues bound to the channel
    def start_consuming(self, queue, queue_name):
        """
        Starts consuming messages from the queue.
        """
        self._consuming = True
        rlog.info(f"Sim {self.sim_name} is starting to consume on {queue_name}.")
    
        self._txq = queue
        self._txq_tag = self._channel.basic_consume(
            queue=self._txq,
            on_message_callback=self._callback,
            auto_ack=True
        )
            
    def on_cancelok(self, _unused_frame, queue_name):
        rlog.info(f"RabbitMQ successfully closed queue {queue_name}")
        
    def send(self, message, dest_node_rxq):
        """
        Publish a message to the destination node's rxq (receive queue)
        """
        rlog.info(f"Publishing message to {dest_node_rxq}")
        self._channel.basic_publish(exchange=self._MAINEXCHANGE, routing_key=dest_node_rxq, body=message)

class SimClient:
    
    def __init__(self, sim_name):
        
        slog.info(f"Initializing NSB sim client, name {sim_name})")
        
        self.sim_name = sim_name
        self._message_queue = []
        
        self._rabbit_manager = SimRabbitManager(self.sim_name, self.__consume_from_txq)
        
        
    def start(self):
        self._rabbit_manager.start()
    
    def __consume_from_txq(self, channel, method, props, body):
        """
        Any node messages sent to the global txq will be consumed, and this callback will be called for every message
        """
        slog.info("Sim Client received message")
        
        data = json.loads(body)
        header = data["header"]
        body = data["body"]
        slog.info(f"Received message from {header['srcid']} to {header['dstid']}")
        
        self.send(header['srcid'], header['dstid'], body)
        
        
        
    def __del__(self):
        """
        Close the connection to the rabbit daemon.
        """
        self.stop()
        
            
    # This function won't stop the main connection the server is running for the brokers,
    # it only stops this client's channel connection to the server
    def stop(self):
        
        self._rabbit_manager.stop()
            
        
    def send(self, src_id, dest_id, message):
        """
        Send a message back to a node
        """
        
         # Create a JSON structure to hold our data in a message
        msg = {
            "header": {
                "type": nsbp.MSG_TYPES.OH_DELIVER_MSG,
                "dataLen": len(message),
                "srcid": src_id,
                "dstid": dest_id
            },
            "body": message
        }
        # Stringify message from json
        msg_str = json.dumps(msg)
        
        # Send the message to the destination NodeClient
        self._rabbit_manager.send(msg_str, f"{dest_id}_rxq")


if __name__ == "__main__":
    # Initialize the SimClient with a unique name
    sim_client = SimClient(sim_name="Simulation_Server_1")
    sim_client.start()
