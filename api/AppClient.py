"""
Send a simple set of byte messages to the server.
"""

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

# Rabbit/Pika
import pika
import pika.exceptions

# Set up logging for server. // logging .INFO by default
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',)
logger = logging.getLogger(f"(client)")
rlog = logging.getLogger(f"(receiver)")
# Set rlog level to DEBUG.
rlog.setLevel(logging.INFO)

# NSB_SERVER_ADDR = "host.docker.internal"
NSB_SERVER_ADDR = nsbp.HOST

"""
HEADER:
Length (4 Bytes)
Source ID (4 Bytes)
Destination ID (4 Bytes)
Message Type (1 Byte)
"""

def ip_to_int(ip):
    """
    Convert an IP address to an integer.
    """
    # FQDN -> IPV4
    ip = socket.gethostbyname(ip)
    # IPV4 -> uint32. The return is a tuple. hence "alias,"
    ip, = struct.unpack("!I", socket.inet_pton(socket.AF_INET, ip))
    return ip

def int_to_ip(ip_int):
    """
    Convert an integer to an IP address.
    """
    # uint32 -> IPV4
    ip = socket.inet_ntop(socket.AF_INET, struct.pack("!I", ip_int))
    return ip

class NSBApplicationClient:
    def __init__(self, clientId=None):
        """
        Initialize the client by creating and maintaining a connection to the server.
        """
        
        self.clientId = clientId
        
        self._correlation_ids = []
        self._connection = None
        self._channel = None
        self._rabbiturl = 'amqp://guest:guest@localhost:15672/%2F'
        self._stopping = False
        
        self._ch_msg_getstate_q = None
        self._ch_recv_msg_q = None
        
        # For receiving in an async environment, store the sent message correlation ids for lookup later
        self._ch_send_msg_messages_corr_ids = dict()
        self._ch_msg_getstate_messages_corr_ids = dict()
        self._ch_recv_msg_messages_corr_ids = dict()
        
        # Create a dict to keep a client copy of each sent message's msgid the server assigned it,
        # mapped to a value 
        self.sent_msg_ids = dict()
        
        self._wait = 0.01
        
    def __del__(self):
        """
        Close the connection to the server.
        """
        self.stop()
        
    
    def start(self):
        
        logger.info(f"Starting client connection for client {self.clientId}")
        
        # Connect to the server and keep attempting to connect if certain connection errors occur
        while not self._stopping:
            try:
                
                # Create our base connection (SelectConnection vs BlockingConnection because we don't want our callbacks to block)
                # On the client, we are running generally assuming server is ran first, so the connection/channel stuff will be using an existing
                # channel/connection vs creating a new one
                # self._connection = pika.SelectConnection(pika.URLParameters(url=self._rabbiturl))
                self._connection = pika.SelectConnection()
                # Create our channel to communicate on
                self._channel = self._connection.channel()
                
                # Redundant code
                
                # Set up the RPC response queue for getting a new message's msgid after its sent, a message's state and receiving simulator messages stored on server
                self._ch_send_msg_q = self._channel.queue_declare(queue='', exclusive=True).method.queue
                self._ch_msg_getstate_q = self._channel.queue_declare(queue='', exclusive=True).method.queue
                self._ch_recv_msg_q = self._channel.queue_declare(queue='', exclusive=True).method.queue
                
                # Set up queue listening for our callback queues above
                self._channel.basic_consume(queue=self._ch_send_msg_q, callback=self.ch_msg_getstate_q_callback, auto_ack=True)
                self._channel.basic_consume(queue=self._ch_msg_getstate_q, callback=self.ch_msg_getstate_q_callback, auto_ack=True)
                self._channel.basic_consume(queue=self._ch_recv_msg_q, callback=self.ch_recv_msg_q_callback, auto_ack=True)
                
                self._connection.ioloop.start()
                
    
            # Don't recover if connection was closed by broker or a client
            except pika.exceptions.ConnectionClosedByBroker:
                self.stop()
                break
            except pika.exceptions.ConnectionClosedByClient:
                self.stop()
                break
            # Don't recover on channel errors
            except pika.exceptions.AMQPChannelError:
                self.stop()
                break
            # Recover on all other connection errors
            except pika.exceptions.AMQPConnectionError:
                continue
            except KeyboardInterrupt:
                self.stop()
                break
            
    # This function won't stop the main connection the server is running for the brokers,
    # it only stops this client's channel connection to the server
    def stop(self):
        
        logger.info(f"Stopping {self.clientId}")
        self._stopping = True
        if self._channel is not None:
            logger.info(f"Closing channel on client with IP: {self.clientId}")
            self._channel.close()
            
    ###
    ### Some RPC callback handlers
    ###
            
    def ch_send_msg_q_callback(self, ch, method, props, body):
        """
        A callback function to handle server replies to our initial ch_send_msg message.
        These replies contain the msgid that was created and assigned to a particular message
        """
        # Put the message in the dictionary with the corresponding correlation id
        logger.info("Client received msgid")
        
        # Add the msgid to self.sent_msg_ids with the original message
        # Body contains the message's associated msgid
        # Delete the temporary correlation_id/message pair
        storedMsg = self.sent_msg_ids[props.correlation_id]
        self.sent_msg_ids[body] = storedMsg
        del self.sent_msg_ids[props.correlation_id]
        
        ch.basic_ack(delivery_tag=props.delivery_tag)
            
    def ch_msg_getstate_q_callback(self, ch, method, props, body):
        """
        A callback function to handle server replies to our initial ch_msg_getstate message
        """
        # Put the message in the dictionary with the corresponding correlation id
        logger.info("Client received state of message")
        # Body contains the status of a message
        self._ch_msg_getstate_messages_corr_ids[props.correlation_id] = body
        
        ch.basic_ack(delivery_tag=props.delivery_tag)
        
    def ch_recv_msg_q_callback(self, ch, method, props, body):
        """
        A callback function to handle server replies to our initial ch_recv_msg message
        """
        # Put the message in the dictionary with the corresponding correlation id
        logger.info("Client received message available from server")
        self._ch_recv_msg_messages_corr_ids[props.correlation_id] = body
        ch.basic_ack(delivery_tag=props.delivery_tag)
    
    ###
    ### END RPC CALLBACK HANDLERS
    ###
        
    def send(self, src_id, dest_id, message, msg_id=None):
        """
        Send a message to the server. 
        An application using this as a client library can pass in an ID for the message being sent,
        and that message's state can be tracked using the same ID. If no ID is provided, the server will 
        automatically assign a client ID to the message, which will be available in self.sent_msg_ids as a mapping between the assigned ID and message
        """
        
        
        # Create a JSON structure to hold our data in a message
        msg = {
            "header": {
                "type": nsbp.MSG_TYPES.CH_SEND_MSG,
                "dataLen": len(message),
                "srcid": src_id,
                "dstid": dest_id,
                "msgid": None,
                "clientmsgid": msg_id
            },
            "body": json.dumps(message)
        }
        # Stringify it and send it to server
        msg_str = json.dumps(msg)
        
        # Generate a corr_id to track the message on the client. This corr_id will 
        # temporarily be used to track the sent message locally until the server can
        # return the tracking id for the message (autoassigned if not provided)
        corr_id = uuid.uuid4()
        
        # Send the message over to the server
        # No need to check for an ack, Rabbit will automatically handle resending if the server doesn't send one
        self._channel.basic_publish(exchange="main_router", routing_key="CH_SEND_MSG", properties=pika.BasicProperties(reply_to=self.ch_send_msg_q_callback, correlation_id=corr_id), body=msg_str)
        
        # Temporarily store the message with corr_id as the key (will be swapped to the client-provided/server-provided ID later)
        self.sent_msg_ids[corr_id] = msg_str 
        logger.info(f"Sent message to server...")
        

    def receive(self, dest_id):
        """
        Receive and return a message from the server.
        """
        rlog.debug(f"Receiving message from server...")
        
        # Create a JSON structure to hold our data in a message
        message = ""
        msg = {
            "header": {
                "type": nsbp.MSG_TYPES.CH_RECV_MSG,
                "dataLen": len(message),
                "srcid": None,
                "dstid": dest_id,
                "msgid": None,
                "clientmsgid": None
            },
            "body": json.dumps(message)
        }
        # Stringify it and send it to server
        msg_str = json.dumps(msg)
        
        corr_id = uuid.uuid4()
        
        # Send the message requesting available messages to the server
        # Since this is an RPC, the response message will go to the ch_recv_msg_q_callback function
        self._channel.basic_publish(exchange="main_router", routing_key="CH_RECV_MSG", body=msg_str, properties=pika.BasicProperties(reply_to=self.ch_recv_msg_q_callback, correlation_id=corr_id))
        
        ## Because Rabbit RPCs won't return to this context directly, let's access the returned message stored in 
        ## self._ch_recv_msg_messages_corr_ids
        ## Because the client is using a SelectConnection (async), we shouldn't block while waiting for a response from the server
        ## that will be stored in self._ch_recv_msg_messages_corr_ids, but due to testing methods below, I'm still writing the functionality to 
        ## block and wait for response in this function. 
        
        # Wait 2% longer each loop to allow more and more time for the server's response to be in the _ch_recv_msg_messages_corr_ids dict
        wait = self._wait
        increment = 1
        while not self._ch_recv_msg_messages_corr_ids[corr_id]:
            time.sleep(wait)
            wait = self._wait * (1 + 0.02) ^ increment
            increment += 1
            # Make sure not an infinite loop
            if (wait > 3):
                return None
            
        # Return the message associated with the original request, and delete it
        temp = self._ch_recv_msg_messages_corr_ids[corr_id]
        del self._ch_recv_msg_messages_corr_ids[corr_id]
        return temp
        
        """
        # Send the message.
        self.sock.send(header)
        rlog.debug(f"\tSent request to server...")
        # Listen for message, but just read the header.
        reply = self.sock.recv(nsbp.CH_HEADER_SIZE)
        rlog.debug(f"\tServer reply: {reply}")
        # Unpack the reply.
        _reply = reply[:nsbp.CH_HEADER_SIZE]
        msg_type, msg_len, src_id, dest_id = struct.unpack(nsbp.CH_HEADER_FORMAT, _reply)
        # Display the values.
        rlog.debug(f"\t\tMessage Type: {msg_type}")
        rlog.debug(f"\t\tMessage Length: {msg_len}")
        rlog.debug(f"\t\tSource ID: {src_id}")
        rlog.debug(f"\t\tDestination ID: {dest_id}")
        # Check for CH_RESP_MSG.
        if msg_type == nsbp.MSG_TYPES.CH_RESP_MSG:
            rlog.debug(f"\tServer sent CH_RESP_MSG.")
            if msg_len > 0:
                # Read the rest of the message.
                msg = self.sock.recv(msg_len)
                rlog.info(f"\tMessage received. Actual length: {len(msg)} / Expected length: {msg_len}")
                # Log source, destination, and message.
                src_addr = int_to_ip(src_id)
                dest_addr = int_to_ip(dest_id)
                rlog.info(f"\t\tSource: {src_addr}")
                rlog.info(f"\t\tDestination: {dest_addr}")
                rlog.info(f"\t\tMessage: {msg}")
                # Return the source, destination, and message.
                return src_addr, dest_addr, msg
            else:
                rlog.debug("No message received.")
        else:
            rlog.error(f"Message not received successfully from the server. Unexpected response.")
        return None
        """
        
    
    def getMessageState(self, msgid):
        """
        Get the state of a message and return the state. For simplification purposes, this function requires a msgid passed in, which is the associated
        id of a message. Client-libs can either provide this themselves or it will be created and returned by the server and stored in
        self.sent_msg_ids as an ID/message mapping. Client-libs can use self.sent_msg_ids to find their message and the associated 
        ID. Client-libs will be responsible for managing the msgid storage themselves
        """
        
        rlog.debug(f"Sending a CH_MSG_GETSTATE message to server...")
        
        # Create a JSON structure to hold our data in a message
        message = ""
        msg = {
            "header": {
                "type": nsbp.MSG_TYPES.CH_MSG_GETSTATE,
                "dataLen": len(message),
                "srcid": None,
                "dstid": None,
                "msgid": None,
                "clientmsgid": msgid
            },
            "body": json.dumps(message)
        }
        # Stringify it and send it to server
        msg_str = json.dumps(msg)
        
        corr_id = uuid.uuid4()
        
        # Send the message requesting the state of a message to the server
        self._channel.basic_publish(exchange="main_router", routing_key="CH_MSG_GETSTATE", body=msg_str, properties=pika.BasicProperties(reply_to=self.ch_msg_getstate_q_callback, correlation_id=corr_id))
        
        
        # Following the same logic in receive()
        # We need to wait for the _ch_msg_getstate_messages_corr_ids dict to hold the status of a message. Once it does, return the status
        # Wait 2% longer each loop to allow more and more time for the server's response to be in the _ch_msg_getstate_messages_corr_ids dict
        wait = self._wait
        increment = 1
        while not self._ch_msg_getstate_messages_corr_ids[corr_id]:
            time.sleep(wait)
            wait = self._wait * (1 + 0.02) ^ increment
            increment += 1
            # Make sure not an infinite loop
            if (wait > 3):
                return None
            
        # Return the status associated with the original request, and delete it
        temp = self._ch_msg_getstate_messages_corr_ids[corr_id]
        del self._ch_msg_getstate_messages_corr_ids[corr_id]
        return temp
        
        

async def test_sender(connector : NSBApplicationClient, aliases : list, auto=False, rate=None, size_bounds=[10, 100]):
    while True:
        # Ensure that if auto is on, rate is not None.
        if auto and rate is None:
            logger.error(f"Auto is on, but rate is None.")
            raise ValueError(f"Auto is on, but rate is None.")
        # Copy list of aliases to a new list.
        this_aliases = aliases.copy()
        # Prompt for the source address.
        src_id = await ainput("Source ID: ") if not auto else ""
        # If the source ID is blank, choose a random address from aliases.
        if src_id == "":
            src_id = random.choice(this_aliases)
        # If the source ID is not blank, check if it is in the aliases.
        elif src_id not in this_aliases:
            logger.error(f"Source ID not in aliases.")
            raise KeyError(f"Source ID not in aliases.")
        else:
            # Display the IP address for the source ID.
            logger.info(f"\t> {src_id}")
        # Remove the source ID from the list of aliases.
        this_aliases.remove(src_id)
        # Prompt for destination address.
        dest_id = await ainput("Destination ID: ") if not auto else ""
        # If the destination ID is blank, choose a random address from aliases.
        if dest_id == "":
            dest_id = random.choice(this_aliases)
        # If the destination ID is not blank, check if it is in the aliases.
        elif dest_id not in this_aliases:
            logger.error(f"Destination ID not in aliases.")
            raise KeyError(f"Destination ID not in aliases.")
        else:
            # Display the IP address for the destination ID.
            logger.info(f"\t> {dest_id}")
        # Prompt for message.
        msg = await ainput("Message: ") if not auto else ""
        # If the message is blank, use a random byte string of random length between 1 and 100.
        if msg == "":
            msg = os.urandom(random.randint(*size_bounds))
            #msg = os.urandom(9) #for testing purpose 
            #msg = os.urandom(random.randint(10, 100))
        else:
            msg = msg.encode()
        # Print the message.
        logger.debug(f"\t> {msg}")

        # Print source, destination and message.
        logger.info(f"Source: {src_id}")
        logger.info(f"Destination: {dest_id}")
        logger.info(f"Message: {msg}")

        # Press enter to continue.
        if not auto:
            await ainput("Press enter to continue...")

        # Send the message.
        result = connector.send(src_id, dest_id, msg)
        if not result:
            logger.info(f"Message sent.")
        else:
            logger.error(f"Message not sent, error occurred.")
        # If auto is True, wait for the rate.
        if auto:
            await asyncio.sleep(1/float(rate))

async def test_receiver(connector : NSBApplicationClient, aliases : list, polling_delay=0.1):
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
            rlog.debug(f"Alias\t> {alias}")
            # Receive a message.
            reply = connector.receive(alias)
            # If the reply is not None, print the message receive information.
            if reply is not None:
                rlog.info(f"\tMessage received.")
                src_addr, dest_addr, msg = reply
                rlog.info(f"\t\tSource: {src_addr}")
                rlog.info(f"\t\tDestination: {dest_addr}")
                # Print only the first 5 bytes of the message, or less.
                rlog.info(f"\t\tMessage: {msg[:min(5, len(msg))]}")
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
    logger.info(f"Aliases:")
    for alias in aliases:
        logger.info(f"\t> {alias}")

    # Create a connector.
    connector = NSBApplicationClient()
    # Gather the test sender and receiver.
    await asyncio.gather(
        test_receiver(connector, aliases),
        test_sender(connector, aliases, size_bounds=size_bounds)
    )

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
    logger.info(f"Aliases:")
    for alias in aliases:
        logger.info(f"\t> {alias}")

    # Create a connector.
    connector = NSBApplicationClient()
    # Gather the test sender and receiver.
    await asyncio.gather(
        test_receiver(connector, aliases),
        test_sender(connector, aliases, auto=True, rate=rate, size_bounds=size_bounds)
    )
    
    

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
        logger.error(f"Map file {map_file_name} does not exist.")
        exit(1)
    # Check if rate is set when auto is on.
    if args.auto and args.rate is None:
        logger.error(f"Rate must be set when auto is on.")
        exit(1)
    # Check that rate is greater than 0 and no more than 1000.
    if args.rate is not None and (args.rate <= 0 or args.rate > 1000):
        logger.error(f"Rate must be greater than 0 and no more than 1000.")
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





    # msg = b"Hello, World!"
    # result = send("10.0.0.4", "10.0.0.5", msg)
    # if not result:
    #     logger.info(f"TEST SUCCESS.")
    # else:
    #     logger.error(f"TEST FAILURE.")
    
