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
# Create a client logger
clog = logging.getLogger(f"(client)")
clog.setLevel(level=logging.INFO)
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


"""
RabbitClient class
Sets up queues, exchanges, etc
Handles stopping, as well as binding consumers

NodeClient class
User client: refers to any code/usage implemented by a user
Node client: refers to our NodeClient code/interface

Class contains send, receive interfaces

During initialization of NodeClient instance (design for multiple instance capabilities), user client can pass in a callback function 
that will be called upon a message being received to this NodeClient, passing in the message. A default `__receive()` function is implemented, which 
stores received messages to a python queue (implemented as a simple list). Calling `receive()` will pop a message from the queue and return it.

NodeClient __init__ needs: [node_id (IPv4 or IPv6 formatted string), node_name (string), opt receive_callback (func)]

Initialization:
- Set and sanity check self.node_id, self.node_name
- Make call to node_client_tx_setup (sets up the queue `send` will send to)
- Create `{self.node_id}_rx` queue, for receiving messages to this NodeClient
- Create consumer for `{self.node_id}_rx` queue (`__receive`)
-- Use callback receive if provided, otherwise store messages which can later be read via `receive()`

TODO: Check if multiple NodeClient's create multiple connections or channels in Rabbit


"""

# The class responsible for managing Rabbit-related stuff
class RabbitManager:
    def __init__(self, node_id, node_name, callback):
        
        self.node_id = node_id
        self.node_name = node_name
        
        self._connection = None
        self._channel = None
        self._rabbiturl = RABBIT_URL
        self._stopping = False
        self._closing_connection = False
        self._MAINEXCHANGE = "main_router"
        self._callback = callback
        
        self._rxq = None
        self._rxq_tag = None
        self._txq = "global_txq" # Any node client or sim client can create the txq 
        

        
    def start(self):
        rlog.info(f"Starting RabbitMQ for node ID {self.node_id}")
        self._stopping = False
        self._closing_connection = False

        try:
            while not self._stopping:
                try:
                    rlog.info("Attempting to establish connection...")
                    self._connection = pika.SelectConnection(
                        pika.URLParameters(self._rabbiturl),
                        on_open_callback=self.on_connection_open,
                        on_open_error_callback=self.on_connection_open_error,
                        on_close_callback=self.on_connection_closed
                    )
                    rlog.info("Node client entering Rabbit IOLoop.")
                    self._connection.ioloop.start()
                except KeyboardInterrupt:
                    rlog.warning("KeyboardInterrupt detected, stopping RabbitManager.")
                    self.stop()
                    break
                except pika.exceptions.AMQPConnectionError as e:
                    rlog.warning(f"AMQPConnectionError: {e}. Retrying...")
        finally:
            if self._connection and not self._connection.is_closed:
                self._connection.ioloop.stop()
        
    def stop(self):
        """Gracefully stop the RabbitMQ connection."""
        rlog.info(f"Stopping node ID {self.node_id}")
        
        if not self._stopping:
            self._stopping = True

        # Close the channel if it's open and prevent double closing
        if self._channel:
            if self._channel.is_open:
                try:
                    cb = functools.partial(self.on_cancelok, queue_name=self._txq)
                    self._channel.basic_cancel(self._txq, cb)
                except pika.exceptions.ChannelWrongStateError:
                    rlog.warning("Channel already closing, skipping cancel.")
                finally:
                    rlog.info(f"Closing channel on node ID: {self.node_id}")
                    self._channel.close()
            else:
                rlog.warning(f"Channel already closed for node ID {self.node_id}")

        # Close the connection if it's still open
        if self._connection:
            if self._connection.is_open:
                rlog.info("Closing the RabbitMQ connection.")
                self._closing_connection = True
                try:
                    self._connection.close()
                    self._connection.ioloop.stop()
                except pika.exceptions.ConnectionWrongStateError:
                    rlog.warning("Connection already in the process of closing.")
            elif self._connection.is_closing:
                rlog.warning("Connection already closing, waiting for shutdown.")
            else:
                rlog.warning("Connection already closed.")
        else:
            rlog.warning("No active RabbitMQ connection to close.")
            
    def on_connection_open(self, connection_obj):
        rlog.info(f"Connection was established for node ID {self.node_id}")
        self.open_channel()
        
    def on_connection_open_error(self,  _unused_connection, err):
        rlog.error(f"Node ID {self.node_id} failed while opening connection: ", err)
        
    def on_connection_closed(self, _unused_connection, reason):
        """
        This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.
        """
        rlog.warning(f"Node ID {self.node_id}'s connection closed unexpectedly: {reason}")
        if self._stopping:
            self.stop()
            
        
    def open_channel(self):
        # Create our channel to handle all messaging
        self._connection.channel(on_open_callback=self.on_channel_open)
    
    def on_channel_open(self, channel):
        rlog.info(f"Node ID {self.node_id}'s channel was successfully opened")
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
        rlog.warning(f"Node ID {self.node_id}'s channel {channel} was closed unexpectedly: {reason}")
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
        self.setup_rx_queue()    # Set up RX queue specific to this node

    def setup_global_txq(self):
        """
        Declares the global TX queue and binds it to the exchange.
        """
        rlog.info(f"Setting up global TX queue: {self._txq}")
        cb = functools.partial(self.on_queue_declareok, queue_name=self._txq)
        self._channel.queue_declare(queue=self._txq, exclusive=False, callback=cb)

    def setup_rx_queue(self):
        """
        Declares the RX queue for the specific node and starts consuming from it.
        """
        rlog.info(f"Node ID {self.node_id} is setting up RX queue: {self.node_id}_rxq")
        rx_cb = functools.partial(self.on_queue_declareok, queue_name=f"{self.node_id}_rxq")
        self._channel.queue_declare(queue=f"{self.node_id}_rxq", exclusive=True, callback=rx_cb)

        
        
    # Bind a queue to the channel (queue_name will be queue-specific, as defined in functools.partial above)
    def on_queue_declareok(self, method_frame, queue_name):
        """
        Handles queue declaration and binds the queue to the exchange.
        """
        rlog.info(f"Queue declared: {queue_name}")
        if queue_name == self._txq:
            # Bind global TX queue to the exchange
            self._channel.queue_bind(queue=self._txq, exchange=self._MAINEXCHANGE)
            rlog.info(f"Global TX queue {queue_name} bound to exchange {self._MAINEXCHANGE}")
        elif queue_name == f"{self.node_id}_rxq":
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
        rlog.info(f"Node ID {self.node_id} is starting to consume on {queue_name}.")
        if queue_name == f"{self.node_id}_rxq":
            self._rxq = queue
            self._rxq_tag = self._channel.basic_consume(
                queue=self._rxq,
                on_message_callback=self.safe_callback,  # Use safe callback wrapper
                auto_ack=True
            )
        else:
            rlog.error("Queue not found.")
            self.stop()
            
    def safe_callback(self, channel, method, properties, body):
        """
        Wrapper for handling callback execution with error handling.
        """
        try:
            self._callback(channel, method, properties, body)
        except Exception as e:
            rlog.error(f"Error in consumer callback: {e}")
            rlog.debug("Message causing error: %s", body)
            
    def on_cancelok(self, _unused_frame, queue_name):
        rlog.info(f"RabbitMQ successfully closed queue {queue_name}")
        
    def send(self, message):
        """
        Publish a message to the global TX queue via the exchange.
        """
        rlog.info(f"Publishing message to global TX queue {self._txq}")
        
        self._channel.basic_publish(exchange=self._MAINEXCHANGE, routing_key=self._txq, body=message)

class NodeClient:
    
    def __init__(self, node_id, node_name, receive_callback=None):
        
        clog.info(f"Initializing NSB node (ID {node_id}, Name {node_name})")
        
        self.node_id = node_id
        self.node_name = node_name
        self._message_queue = []
        self._receive_callback = receive_callback or self.__default_receive_callback
        
        self._rabbit_manager = RabbitManager(self.node_id, self.node_name, self.__receive)
        
    def start(self):
        self._rabbit_manager.start()
            
    # The internal receive for a NodeClient instance, users should NOT call this
    # The Rabbit rx consumer will call this method
    def __receive(self, channel, method, props, body):
        # Either store message in self._message_queue or call the user's provided callback
        try:
            if self._receive_callback is self.__default_receive_callback:
                self.__default_receive_callback(body)
            else:
                self._receive_callback(body)
        except Exception as e:
            clog.error(f"Error in user-provided callback: {e}")
            clog.debug("Message causing error: %s", body)
            
    def __default_receive_callback(self, message):
        """
        Default method for handling received messages.
        """
        self._message_queue.append(message)
        
    def receive(self):
        """
        Default receive method that user client can call to return received messages
        """
        if self._message_queue:
            return self._message_queue.pop(0)
        else:
            clog.warning("Cannot return message from empty queue")
            return
        
        
    def __del__(self):
        """
        Close the connection to the rabbit daemon.
        """
        self.stop()
        
            
    # This function won't stop the main connection the server is running for the brokers,
    # it only stops this client's channel connection to the rabbit daemon
    def stop(self):
        
        self._rabbit_manager.stop()
            
        
    def send(self, dest_id, message):
        """
        Send a message to the sim client. 
        An application using this as a client library can pass in an ID for the message being sent,
        and that message's state can be tracked using the same ID. If no ID is provided, the server will 
        automatically assign a client ID to the message, which will be available in self.sent_msg_ids as a mapping between the assigned ID and message
        """

        # Create a JSON structure to hold our data in a message
        msg = {
            "header": {
                "type": nsbp.MSG_TYPES.CH_SEND_MSG,
                "dataLen": len(message),
                "srcid": self.node_id,
                "dstid": dest_id, 
            },
            "body": json.dumps(message)
        }
        # Stringify message from json
        msg_str = json.dumps(msg)
        
        # Send the message
        self._rabbit_manager.send(msg_str)
        
        
"""
Because we want to support one NodeClient interface with multiple user client connections (aka sending/receiving from multiple nodes/IPs),
we need to make a special class that supports dynamic source node IPs/names, that can also receive from multiple nodes and appropriately multiplex the 
single shared rabbit receiver queue.

self.node_id_list = []
send()

"""
class SharedNodeClient:
    
    def __init__(self, node_id, node_name, receive_callback=None):
        
        clog.info(f"Initializing NSB node (ID {node_id}, Name {node_name})")
        
        self.node_id = node_id
        self.node_name = node_name
        self._message_queue = []
        self._receive_callback = receive_callback or self.__default_receive_callback
        
        self._rabbit_manager = RabbitManager(self.node_id, self.node_name, self.__receive)
        
            
    # The internal receive for a NodeClient instance, users should NOT call this
    # The Rabbit rx consumer will call this method
    def __receive(self, channel, method, props, body):
        # Either store message in self._message_queue or call the user's provided callback
        try:
            if self._receive_callback is self.__default_receive_callback:
                self.__default_receive_callback(body)
            else:
                self._receive_callback(body)
        except Exception as e:
            clog.error(f"Error in user-provided callback: {e}")
            clog.debug("Message causing error: %s", body)
            
    def __default_receive_callback(self, message):
        """
        Default method for handling received messages.
        """
        self._message_queue.append(message)
        
    def receive(self):
        """
        Default receive method that user client can call to return received messages
        """
        if self._message_queue:
            return self._message_queue.pop(0)
        else:
            clog.warning("Cannot return message from empty queue")
            return
        
        
    def __del__(self):
        """
        Close the connection to the rabbit daemon.
        """
        self.stop()
        
            
    # This function won't stop the main connection the server is running for the brokers,
    # it only stops this client's channel connection to the rabbit daemon
    def stop(self):
        
        self._rabbit_manager.stop()
            
        
    def send(self, dest_id, message):
        """
        Send a message to the sim client. 
        An application using this as a client library can pass in an ID for the message being sent,
        and that message's state can be tracked using the same ID. If no ID is provided, the server will 
        automatically assign a client ID to the message, which will be available in self.sent_msg_ids as a mapping between the assigned ID and message
        """

        # Create a JSON structure to hold our data in a message
        msg = {
            "header": {
                "type": nsbp.MSG_TYPES.CH_SEND_MSG,
                "dataLen": len(message),
                "srcid": self.node_id,
                "dstid": dest_id
            },
            "body": json.dumps(message)
        }
        # Stringify message from json
        msg_str = json.dumps(msg)
        
        # Send the message
        self._rabbit_manager.send(msg_str)
        
        

async def test_sender( aliases : list, auto=False, rate=None, size_bounds=[10, 100]):
    clog.info("Starting test sender..")
    
    # Copy list of aliases to a new list.
    this_aliases = aliases.copy()
    
    # Create a dictionary of NodeClients 
    node_clients = {}
    for alias in this_aliases:
        node_clients[alias] = NodeClient(alias, f"{alias}_node")
        node_clients[alias].start()
    
    
    while True:
        
        # Ensure that if auto is on, rate is not None.
        if auto and rate is None:
            clog.error(f"Auto is on, but rate is None.")
            raise ValueError(f"Auto is on, but rate is None.")
            exit(1)
            
        # Copy list of aliases to a new list.
        this_aliases = aliases.copy()
        
        for alias in this_aliases:
            
            src_id = alias
            
            # Prompt for destination address.
            dest_id = await ainput("Destination ID: ") if not auto else ""
            # If the destination ID is blank, choose a random address from aliases.
            while dest_id == "" or dest_id == src_id:
                dest_id = random.choice(this_aliases)
                
            # Prompt for message.
            msg = await ainput("Message: ") if not auto else f"{src_id} is sending a message to {dest_id} at {str(time.time())}"

            # Print source, destination and message.
            clog.info(f"Source: {src_id}")
            clog.info(f"Destination: {dest_id}")
            clog.info(f"Message: {msg}")

            # Press enter to continue.
            if not auto:
                await ainput("Press enter to continue...")

            # Send the message.
            connector = node_clients[alias]
            connector.send(dest_id, msg)
            clog.info(f"Message sent.")
            
            
        # If auto is True, wait for the rate.
        if auto:
            await asyncio.sleep(1/float(rate))

async def test_receiver(connector : NodeClient, aliases : list, polling_delay=1):
    """
    This test receiver will cycle through the aliases and receive messages.
    """
    rlog.info(f"Starting test receiver...")
    while True:
        # Copy list of aliases to a new list.
        this_aliases = aliases.copy()
        # Loop through the aliases.
        for alias in this_aliases:
            # Print the alias.
            clog.debug(f"Alias\t> {alias}")
            # Receive a message.
            reply = connector.receive(alias)
            # If the reply is not None, print the message receive information.
            if reply is not None:
                clog.info(f"Receiver Reply: {reply.decode()}")
            # If the reply is None, print that no message was received.
            else:
                rlog.debug(f"\t\tNo message received.")
        # Sleep to create a delay between receiving messages.
        await asyncio.sleep(polling_delay)
        


async def main_manual(map_file_name, size_bounds):

    # Get list of addresses from aliasmap.txt.
    with open(map_file_name, "r") as f:
        lines = f.readlines()
    # Create a dictionary of aliases.
    aliasmap = {}
    for line in lines:
        alias, ip = line.split(':')
        alias = alias.strip()
        ip = ip.strip()
        aliasmap[alias] = ip
    # Create a list of the keys.
    aliases = list(aliasmap.keys())
    # Print the list of aliases, but in a pretty way.
    clog.info(f"Aliases:")
    for alias in aliases:
        clog.info(f"\t> {alias}")

    # Create a connector.
    connector = NodeClient("10.0.0.1", "Node 1")
    connector.start()
    # Gather the test sender and receiver.
    try:
        await asyncio.gather(
            # test_receiver(connector, aliases),
            test_sender(connector, aliases, size_bounds=size_bounds)
        )
    except:
        connector.stop()

async def main_auto(map_file_name, rate, size_bounds):
    # Get list of addresses from aliasmap.txt.
    with open(map_file_name, "r") as f:
        lines = f.readlines()
    # Create a dictionary of aliases.
    aliasmap = {}
    for line in lines:
        alias, ip = line.split(':')
        alias = alias.strip()
        ip = ip.strip()
        aliasmap[alias] = ip
    # Create a list of the keys.
    aliases = list(aliasmap.keys())
    # Print the list of aliases, but in a pretty way.
    clog.info(f"Aliases:")
    for alias in aliases:
        clog.info(f"\t> {alias}")

    # Gather the test sender and receiver.
    time.sleep(2)
    try: 
        await asyncio.gather(
            test_sender(aliases, auto=True, rate=rate, size_bounds=size_bounds)
        )
    except Exception as e:
        clog.error(f"Error occurred in gather of test_sender: {e}")
    
    

# Run the main function.
if __name__ == "__main__":
    import argparse
    # Use argparse to get the map file name.
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--map_file_name", help="The name of the alias map file.", required=True)
    parser.add_argument("-a", "--auto", help="Automatically send messages at selected rate.", action="store_true")
    parser.add_argument("-r", "--rate", help="The rate at which to send messages in messages/second (0, 1000]).", type=float, default=10)
    # Pass in two values as bounds for the random message size.
    parser.add_argument("-b", "--bounds", help="The bounds for the random message size.", nargs=2, type=int, default=[10, 100])
    args = parser.parse_args()
    map_file_name = args.map_file_name
    # Check if the map file exists.
    if not os.path.exists(map_file_name):
        clog.error(f"Map file {map_file_name} does not exist.")
        exit(1)
    # Check if rate is set when auto is on.
    if args.auto and args.rate is None:
        clog.error(f"Rate must be set when auto is on.")
        exit(1)
    # Check that rate is greater than 0 and no more than 1000.
    if args.rate is not None and (args.rate <= 0 or args.rate > 1000):
        clog.error(f"Rate must be greater than 0 and no more than 1000.")
        exit(1)
    # We will use a try/except block to catch the KeyboardInterrupt.
    try:
        """
        Once we have defined our main coroutine, we will run it using asyncio.run().
        """
        if not args.auto:
            asyncio.run(main_manual(map_file_name, args.bounds))
        else:
            asyncio.run(main_auto(map_file_name, args.rate, args.bounds))
    except KeyboardInterrupt:
        """
        If the user presses Ctrl+C, we will gracefully exit the program.
        """
        print("Exiting program...")
        exit(0)




# Example of a SharedRabbitManager, in the event that connections can bog down system
# I did not test this, so potential bugs/errors might occur in the event of a direct swap

# class SharedRabbitManager:
#     def __init__(self):
#         self._connection = None
#         self._channel = None
#         self._clients = {}  # Track NodeClients and their RX queues
#         self._setup_done = False
#         self._rabbiturl = RABBIT_URL
#         self._MAINEXCHANGE = "main_router"

#     def setup(self):
#         if not self._setup_done:
#             self._connection = pika.SelectConnection(
#                 pika.URLParameters(self._rabbiturl),
#                 on_open_callback=self.on_connection_open,
#                 on_open_error_callback=self.on_connection_open_error,
#                 on_close_callback=self.on_connection_closed
#             )
#             self._setup_done = True

#     def register_client(self, node_id, callback):
#         """
#         Register a NodeClient with its RX queue.
#         """
#         if node_id in self._clients:
#             raise ValueError(f"NodeClient {node_id} already registered.")
#         self._clients[node_id] = callback
#         self._setup_rx_queue(node_id)

#     def _setup_rx_queue(self, node_id):
#         """
#         Declare an RX queue for the given NodeClient.
#         """
#         queue_name = f"{node_id}_rxq"
#         self._channel.queue_declare(queue=queue_name, exclusive=True)
#         self._channel.queue_bind(
#             queue=queue_name,
#             exchange=self._MAINEXCHANGE,
#             routing_key=queue_name
#         )
#         self._channel.basic_consume(
#             queue=queue_name,
#             on_message_callback=lambda ch, method, props, body: self._safe_callback(node_id, body),
#             auto_ack=True
#         )

#     def _safe_callback(self, node_id, body):
#         """
#         Dispatch messages to the appropriate NodeClient's callback.
#         """
#         try:
#             if node_id in self._clients:
#                 self._clients[node_id](body)
#             else:
#                 rlog.warning(f"Received message for unknown NodeClient {node_id}: {body}")
#         except Exception as e:
#             rlog.error(f"Error in callback for NodeClient {node_id}: {e}")

# # Example Usage
# shared_manager = SharedRabbitManager()
# node_client_1 = NodeClient("192.168.1.1", "Node1", shared_manager)
# node_client_2 = NodeClient("192.168.1.2", "Node2", shared_manager)
