import sys
from os import remove
from select import select
import socket
import selectors
import types
import copy
import time
import functools

from ctypes import *
import nsb_payload as nsbp
import struct
import json

import logging

# Rabbit
import pika

logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s: %(message)s',)
# Client logger.
clog = logging.getLogger('(client)')
# INFO by default
clog.setLevel(logging.INFO)
# Server logger.
slog = logging.getLogger('(server)')
# INFO by default
slog.setLevel(logging.INFO)

# Create message counter to generate IDs.
G_MSGID             = -1

DEBUG = True

def get_msg_id():
    global G_MSGID
    G_MSGID += 1
    return G_MSGID

class ClientNotFoundError(Exception):
    pass

class Header(object):
    def __init__(self):
        self.type = None
        self.dataLen = 0
        self.srcid = None
        self.dstid = None
        self.msgid = None

    def __str__(self):
        return "type %s dataLen %s srcid %s dstid %s msgid %s" % \
            (self.type, self.dataLen, self.srcid, self.dstid, self.msgid)

class Msg(object):
    def __init__(self, id_, header, data):
        self.id = id_
        self.q = None
        self.header = copy.copy(header)
        self.data = copy.copy(data)
    
    def setq(self, q):
        self.q = q

class Client(object):
    def __init__(self, clientIp, nodeId, server):
        """
        Initialize client object with the app (IP) and sim (node ID) representations.
        Sets up various queue memberships. Also saves the server. Creates a per-client 
        buffer system. Clients just store messages and their states, they do not track
        sources and destinations.
        """
        clog.info(f"Initializing client {clientIp} with node ID {nodeId}...")
        self.clientIp = clientIp
        self.nodeId = nodeId
        self.server = server
        # Contain each client's messages sent (store the IDs) and the associated message
        self.msg_id_map = dict()
        self.txq = list()
        self.transitq = list()
        self.rxq = list()
        
        clog.info(f"\tClient {clientIp} initialized.")

    def add2q(self, msg, dstq):
        clog.debug(f"Adding message {msg.id} to queue {dstq}...")
        # Check destination queue and validate message state transitions.
        if dstq == nsbp.MSG_Q.MSGQ_TX:
            # Transmit queue.
            clog.debug(f"\tAdding message {msg.id} to TX queue...")
            # Check that message is new and does not exist in any queue or the map.
            assert(msg.q == None)
            assert (msg.id not in self.msg_id_map)
            assert (msg.id not in self.txq)
            # Add message to map and queue.
            self.msg_id_map[msg.id] = msg
            self.txq.append(msg.id)
            clog.debug(f"\tMessage {msg.id} added.")
        elif dstq == nsbp.MSG_Q.MSGQ_TRANSIT:
            # Transit queue.
            clog.debug(f"\tAdding message {msg.id} to transit queue...")
            # Check that message is in the transmit queue, and not in transit.
            assert (msg.q == nsbp.MSG_Q.MSGQ_TX)
            assert(msg.id in self.msg_id_map)
            assert (msg.id not in self.transitq)
            # Add to transit queue.
            self.transitq.append(msg.id)
            clog.debug(f"\tMessage {msg.id} added.")
        elif dstq == nsbp.MSG_Q.MSGQ_RX:
            # Receive queue.
            clog.debug(f"\tHandling message {msg.id} in RX queue...")
            # Check that message state is valid.
            if msg.id in self.msg_id_map or \
              msg.id in self.rxq:
                clog.debug(f"\tMessage {msg.id} already delivered.")
                return nsbp.ERROR_CODES.MESSAGE_ALREADY_DELIVERED
            # Check that message is in transit.
            if msg.q != nsbp.MSG_Q.MSGQ_TRANSIT:
                clog.debug(f"\tMessage {msg.id} in wrong state.")
                return nsbp.ERROR_CODES.MESSAGE_IN_WRONG_STATE
            # Add to receive queue.
            self.msg_id_map[msg.id] = msg
            self.rxq.append(msg.id)
            clog.debug(f"\tMessage {msg.id} added to RX queue.")
        # Set message queue state.
        msg.setq(dstq)
        return nsbp.ERROR_CODES.SUCCESS

    def getMsgState(self, msgid):
        """
        Returns the state of the message.
        """
        # Check if message exists.
        if msgid not in self.msg_id_map:
            return nsbp.MSG_STATE.MSG_STATE_NOTFOUND
        # Check the message queue state.
        if self.msg_id_map[msgid].q == nsbp.MSG_Q.MSGQ_TX:
            assert(msgid in self.txq)
            return nsbp.MSG_STATE.MSG_STATE_QUEUED
        elif self.msg_id_map[msgid].q == nsbp.MSG_Q.MSGQ_TRANSIT:
            assert(msgid in self.transitq)
            return nsbp.MSG_STATE.MSG_STATE_INTRANSIT
        elif self.msg_id_map[msgid].q == nsbp.MSG_Q.MSGQ_RX:
            assert(msgid in self.rxq)
            return nsbp.MSG_STATE.MSG_STATE_DELIVERED
        # Should never get here.
        assert(False) 

    def createNewMessage(self, header, pktData):
        """
        Creates a new message and adds it to the transmit queue.
        """
        clog.info(f"Creating new message from {header.srcid} to {header.dstid}...")
        # Check that message is not to self.
        if (header.srcid == header.dstid):
            return (nsbp.ERROR_CODES.MESSAGE_TO_SELF, None)
        if not header.msgid: return (nsbp.ERROR_CODES.NO_PROVIDED_MSGID, msg)
        msg = Msg(header.msgid, header, pktData)
        # Add to transmit queue.
        self.add2q(msg, nsbp.MSG_Q.MSGQ_TX)
        # Log.
        clog.info(f"\tMessage {msg.id} ({len(msg.data)} B) created.")
        clog.debug(f"Transmit Queue:\n{self.txq}")
        return (nsbp.ERROR_CODES.SUCCESS, msg)

    def sendChMsgAck(self, sock, header, msgid, returnCode):
        """
        Sends a message acknowledgement.
        """
        clog.debug(f"Sending ack for message {msgid}...")
        # Create format.
        fmt = nsbp.CH_HEADER_FORMAT + nsbp.CH_SEND_MSG_ACK_FORMAT
        # Create ack buffer.
        ackbuf = struct.pack(fmt,
            nsbp.MSG_TYPES.CH_SEND_MSG_ACK,
            nsbp.CH_SEND_MSG_ACK_SIZE, header.srcid,
            header.dstid, returnCode, msgid)
        # Send ack.
        sock.sendall(ackbuf)
        clog.debug(f"\tAck sent.")

    def sendChMsgState(self, sock, pktData):
        """
        Sends the state of a message.
        """
        # clog.debug("GET STATE UNPACK MSG LEN %d" % len(pktData))
        clog.info(f"Handling request to get state...")
        clog.debug(f"\tUnpacking request of length {len(pktData)}...")
        # Unpacking message to get message ID.
        msgid, = struct.unpack("="+nsbp.CH_MSG_GETSTATE_FORMAT, pktData)
        clog.debug(f"\tFound message ID: {msgid}")
        # Get message state.
        state = self.getMsgState(msgid)
        # Would need to make sure it can handle state of None
        clog.debug(f"\tMessage state: {state}")
        # Create format.
        fmt = nsbp.CH_HEADER_FORMAT + nsbp.CH_MSG_STATE_FORMAT
        # Create response buffer.
        statebuf = struct.pack(fmt,
            nsbp.MSG_TYPES.CH_MSG_STATE,
            nsbp.CH_MSG_STATE_SIZE, 0, 0, state, msgid)
        # Send response.
        sock.sendall(statebuf)
        clog.info(f"\tResponse with message ID {msgid} at state {state} sent.")

    def sendChRespMsg(self, sock):
        """
        Sends a response to the application's request for received messages.
        """
        clog.debug(f"Handling Client request for received messages...")
        pktData = ''.encode()
        # Check if there are any messages to send.
        if len (self.rxq) == 0:
            clog.debug(f"\tNo messages to send.")
            # If not, send a response with no data.
            fmt = "%s%ss" % (nsbp.CH_HEADER_FORMAT, len(pktData))
            # if DEBUG: print ("FORMAT %s" % fmt)
            retbuf = struct.pack(fmt, nsbp.MSG_TYPES.CH_RESP_MSG,
                len(pktData), 0, 0, pktData)
        else:
            clog.info(f"\tMessage found. Forwarding to app at {self.rxq[0]}...")
            # If there are messages, send the first one (packed).
            msgid = self.rxq.pop(0)
            msg = self.msg_id_map[msgid]
            fmt = "%s%ss" % (nsbp.CH_HEADER_FORMAT, len(msg.data))
            # if DEBUG: print ("FORMAT %s" % fmt)
            retbuf = struct.pack(fmt, nsbp.MSG_TYPES.CH_RESP_MSG,
                len(msg.data), msg.header.srcid, msg.header.dstid, msg.data)
            # Remove message from message map.
            self.msg_id_map.pop(msgid)
            clog.info(f"\tMessage {msgid} retrieved and removed from message map.")
        clog.debug(f"\tResponse buffer length: {len(retbuf)}")
        sock.sendall(retbuf)
        clog.debug(f"\tResponse sent.")
        
    def getChRespMsg(self):
        """
        Similar to sendChRespMsg, but gets the message and returns it
        """
        clog.debug(f"Handling Client request for received messages...")

        # Check if there are any messages to send.
        if len (self.rxq) == 0:
            clog.debug(f"\tNo messages to send.")
            # If not, send a response with no data.
            return None
        else:
            clog.info(f"\tMessage found. Forwarding to app client at {self.rxq[0]}...")
            # If there are messages, send the first one (packed).
            msgid = self.rxq.pop(0)
                
        
            msg = self.msg_id_map[msgid]
            # if DEBUG: print ("FORMAT %s" % fmt)
            retbuf = struct.pack(fmt, nsbp.MSG_TYPES.CH_RESP_MSG,
                len(msg.data), msg.header.srcid, msg.header.dstid, msg.data)
            # Remove message from message map.
            self.msg_id_map.pop(msgid)
                
            returnMsg = {
                "header": {
                    "type": nsbp.MSG_TYPES.CH_RESP_MSG,
                    "dataLen": len(msg.data),
                    "srcid": msg.header.srcid,
                    "dstid": msg.header.dstid,
                    "msgid": msg.header.msgid
                },
                "body": json.dumps(msg.data)
            }
            
            clog.info(f"\tMessage {msgid} retrieved and removed from message map.")
            
            return returnMsg

    def sendOhRespMsg(self, sock):
        """
        Sends a response to the simulator's request for outgoing messages.
        """
        clog.debug(f"Handling sim request for outgoing messages...")
        pktData = ''.encode()
        # Check if there are any messages to send over the simulated network.
        if len (self.txq) == 0:
            clog.debug(f"\tNo messages to send.")
            fmt = "%s%ss" % (nsbp.OH_HEADER_FORMAT, len(pktData))
            # if DEBUG: print ("FORMAT %s" % fmt)
            retbuf = struct.pack(fmt, nsbp.MSG_TYPES.OH_RESP_MSG,
                len(pktData), 0, 0, 0, pktData)
        else:
            # If there are outgoing messages, send the first one (packed).
            clog.info(f"\tMessage found. Sending...")
            msgid = self.txq.pop(0)
            msg = self.msg_id_map[msgid]
            fmt = "%s%ss" % (nsbp.OH_HEADER_FORMAT, len(msg.data))
            # if DEBUG: print ("FORMAT %s" % fmt)
            # Set the destination client using the reference.
            dstClient = server.ip_reference[msg.header.dstid]
            # Pack message.
            retbuf = struct.pack(fmt, nsbp.MSG_TYPES.OH_RESP_MSG,
                len(msg.data), self.nodeId, dstClient.nodeId, msgid, msg.data)
            # Add message to transit queue.
            self.add2q(msg, nsbp.MSG_Q.MSGQ_TRANSIT)
        clog.debug(f"\tResponse buffer length: {len(retbuf)}")
        sock.sendall(retbuf)
        clog.debug(f"\tResponse sent.")

    def transitq2rxq (self, srcClient, header, pktData):
        """
        Moves a message from the src transit queue to the dest received queue.
        """
        clog.info(f"Handling message delivery in queues...")
        # Check if message is in sender's transit queue and message map.
        if header.msgid not in srcClient.msg_id_map:
            return nsbp.ERROR_CODES.MESSAGE_NOT_FOUND
        if header.msgid not in srcClient.transitq:
            return nsbp.ERROR_CODES.MESSAGE_IN_WRONG_QUEUE
        # Remove message from sender's transit queue and message map.
        index = srcClient.transitq.index(header.msgid)
        msgid = srcClient.transitq.pop(index)
        msg = srcClient.msg_id_map.pop(msgid)
        # Add message to receiver's received queue and message map.
        rc = self.add2q(msg, nsbp.MSG_Q.MSGQ_RX)
        # Return result code.
        return rc

    def sendOhDeliveryMsgAck(self, sock, header, returnCode):
        """
        Sends a message delivery acknowledgement to the simulator.
        """
        clog.info(f"Sending delivery ack to simulator...")
        # Create and send acknowledgment.
        fmt = nsbp.OH_HEADER_FORMAT + nsbp.OH_DELIVER_MSG_ACK_FORMAT
        ackbuf = struct.pack(fmt,
            nsbp.MSG_TYPES.OH_DELIVER_MSG_ACK,
            nsbp.OH_DELIVER_MSG_ACK_SIZE, header.srcid,
            header.dstid, header.msgid, returnCode)
        sock.sendall(ackbuf)
        clog.info(f"\tAck sent.")
        
"""
NSBServerTracker Class:

This class is used to track network metrics for the NSB server. It is optionally 
created and utilized by the NSB server to keep track of latency and throughput 
by recording the elapsed time between the time a message is sent and the time 
it is received. It also keeps track of the size of the messages sent and received.
"""

class NSBServerTrackerEntry:
    """
    This class is used to track the latency and throughput of a single message.
    """
    def __init__(self, msgid, src, dst, size):
        """
        Initializes the entry with the message id, source, destination, and size.
        Also sets the start time to the current time.
        """
        self.msgid = msgid
        self.src = src
        self.dst = dst
        self.size = size
        # Start time is set when the message is sent.
        self.start_time = time.time()
        self.end_time = None
        self.latency = None
    def end(self):
        """
        Ends the entry by setting the end time and calculating the latency.
        """
        self.end_time = time.time()
        self.latency = self.end_time - self.start_time

class NSBServerTracker:
    """
    This class is the main class for the NSB server tracker. It is used to keep 
    track of the single message entries, and then to perform calculations.
    """
    def __init__(self):
        """
        Initializes the tracker, which is just a dictionary.
        """
        self.entries = dict()
    def addMessage(self, msgid, src, dst, size):
        """
        Adds a new entry to the tracker.
        """
        self.entries[msgid] = NSBServerTrackerEntry(msgid, src, dst, size)
    def endMessage(self, msgid):
        """
        Ends an entry in the tracker.
        """
        self.entries[msgid].end()
    def getLatency(self, msgid):
        """
        Returns the latency of a message.
        """
        return self.entries[msgid].latency
    def cleanIncompleteEntries(self):
        """
        Removes all entries that have not been ended.
        """
        to_delete = []
        for msgid in self.entries:
            if self.entries[msgid].end_time == None or self.entries[msgid].latency == None:
                # Add to list of entries to remove.
                to_delete.append(msgid)
        # Why not just del self.entries[msgid] above?
        for msgid in to_delete:
            del self.entries[msgid]
        # Return the the number of remaining entries and number of entries removed.
        return len(self.entries), len(to_delete)
    def getAverageLatency(self):
        """
        Returns the average latency of all messages.
        """
        total_latency = 0
        for msgid in self.entries:
            total_latency += self.entries[msgid].latency
        return total_latency / len(self.entries)
    def getTotalThroughput(self):
        """
        Returns the throughput of all messages.
        """
        total_size = 0
        total_time = 0
        for msgid in self.entries:
            total_size += self.entries[msgid].size
            total_time += self.entries[msgid].latency
        return total_size / total_time
    def getNumberOfMessages(self):
        """
        Returns the number of messages tracked.
        """
        return len(self.entries)
    

"""
NSBServer Class:

This is the main class for the NSB server. It includes its own initialization 
and the initialization of the clients, and optionally of the network tracking
class. It also includes the main event loop for the server.
"""

class NSBServer(object):
    """
    NSBServer: Initialization, Helper, and Runtime Methods
    """
    def __init__(self, map_file_name, track_network_metrics=False, message_limit=0):
        """
        Sets up host/port and initializes buffers.
        """
        slog.info(f"Initializing server...")
        # Set selector.
        self.sel = selectors.DefaultSelector()
        self.clients = dict()
        # Initialize all the clients.
        slog.info(f"\tInitializing clients...")
        self.init_clients(map_file_name, nodeid_type="ip")
        slog.info(f"\tClients initialized.")
        # Initialize network tracker.
        self.track_network_metrics = track_network_metrics
        if self.track_network_metrics:
            slog.info(f"\tInitializing network tracker...")
            self.tracker = NSBServerTracker()
            # Set message limit.
            self.message_limit = message_limit
            slog.info(f"\tNetwork tracker initialized.")
        slog.info(f"Server initialized.")
        
        # Set up Rabbit-related properties
        self._correlation_ids = []
        self._connection = None
        self._channel = None
        self._rabbiturl = "amqp://guest:guest@localhost:5672/%2F"
        self._consuming = False
        self._stopping = False
        self._MAINEXCHANGE = "main_router"
        
        # Store the various queues and their tag IDs here
        self._ch_send_msg_q = None
        self._ch_msg_getstate_q = None
        self._ch_recv_msg_q = None
        self._oh_recv_msg_q = None
        self._oh_deliver_msg_q = None
        self._ch_send_msg_q_tag = None
        self._ch_msg_getstate_q_tag = None
        self._ch_recv_msg_q_tag = None
        self._oh_recv_msg_q_tag = None
        self._oh_deliver_msg_q_tag = None
        
    def on_connection_open(self, connection_obj):
        slog.info("Connection was established.")
        self.open_channel()
        
    def on_connection_open_error(self,  _unused_connection, err):
        slog.error("Server failed while opening connection: ", err)
        # LOGGER.error('Connection open failed, reopening in 5 seconds: %s', err)
        # self._connection.ioloop.call_later(5, self._connection.ioloop.stop) 
        
    def on_connection_closed(self, _unused_connection, reason):
        """
        This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.
        """
        slog.warning(f"Server connection closed unexpectedly: {reason}")
        if self._stopping:
            self.stop()
        else:
            slog.warning("Server will attempt to reconnect.")
            # Add reconnect code here
            
        
    def open_channel(self):
        slog.info("Opening channel...")
        # Create our channel to handle all messaging
        self._connection.channel(on_open_callback=self.on_channel_open)
    
    def on_channel_open(self, channel):
        # Store the channel object in self._channel
        slog.info("Channel opened.")
        self._channel = channel
        # Add a channel closed callback to the channel
        self._channel.add_on_close_callback(self.on_channel_closed)
        # Setup the main exchange with some name
        self.setup_exchange()
        
    def on_channel_closed(self, channel, reason):
        """
        Invoked by pika when RabbitMQ unexpectedly closes the channel.
        """
        slog.warning('Channel %i was closed unexpectedly: %s', channel, reason)
        self._channel = None
        if not self._stopping:
            self._connection.close()
         
    # Will set up a direct exchange called "main_router" if thats what self._MAINEXCHANGE is set to.
    # This will lead to func that will set 5 queues up, use carefully
    def setup_exchange(self):
        slog.info(f"Setting up exchange {self._MAINEXCHANGE}")
        cb = functools.partial(self.on_exchange_declareok, exchange_name=self._MAINEXCHANGE)
        self._channel.exchange_declare(exchange=self._MAINEXCHANGE, exchange_type="direct", callback=cb)
        
    # When the exchange was correctly declared
    def on_exchange_declareok(self, _unused_frame, exchange_name):
        """
        Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.
        """
        slog.info('Exchange declared: %s', exchange_name)
        self.setup_queues()
        
    # Set up our 5 consumer queues to monitor
    # Start consuming in this same function
    def setup_queues(self):
        
        slog.info("Server is setting up various queues.")
        
        cb = functools.partial(self.on_queue_declareok, queue_name="CH_SEND_MSG_Q", routing_key="CH_SEND_MSG")
        self._ch_send_msg_q = "CH_SEND_MSG_Q"
        self._channel.queue_declare(queue="CH_SEND_MSG_Q", callback=cb)
        cb = functools.partial(self.on_queue_declareok, queue_name="CH_MSG_GETSTATE_Q", routing_key="CH_MSG_GETSTATE")
        self._ch_msg_getstate_q = "CH_MSG_GETSTATE_Q"
        self._channel.queue_declare(queue="CH_MSG_GETSTATE_Q", callback=cb)
        cb = functools.partial(self.on_queue_declareok, queue_name="CH_RECV_MSG_Q", routing_key="CH_RECV_MSG")
        self._ch_recv_msg_q = "CH_RECV_MSG_Q"
        self._channel.queue_declare(queue="CH_RECV_MSG_Q", callback=cb)
        cb = functools.partial(self.on_queue_declareok, queue_name="OH_RECV_MSG_Q", routing_key="OH_RECV_MSG")
        self._oh_recv_msg_q = "OH_RECV_MSG_Q"
        self._channel.queue_declare(queue="OH_RECV_MSG_Q", callback=cb)
        cb = functools.partial(self.on_queue_declareok, queue_name="OH_DELIVER_MSG_Q", routing_key="OH_DELIVER_MSG")
        self._oh_deliver_msg_q = "OH_DELIVER_MSG_Q"
        self._channel.queue_declare(queue="OH_DELIVER_MSG_Q", callback=cb)
        
        # Set the qos and then start consuming
        self._channel.basic_qos(prefetch_count=1)
        
        
    # Bind a queue to the channel (queue_name and routing_key will be queue-specific, as defined in functools.partial above)
    def on_queue_declareok(self, _unused_frame, queue_name, routing_key):
        
        slog.info(f"Queue {queue_name} with routing_key {routing_key} successfully declared.")
        # Bind our queue to the channel
        cb = functools.partial(self.on_bindok, queue_name=queue_name)
        self._channel.queue_bind(queue=queue_name, exchange=self._MAINEXCHANGE, routing_key=routing_key, callback=cb)
        
    # If the bind succeeded, log the success
    def on_bindok(self, _unused_frame, queue_name):
        
        slog.info(f"Successfully bound queue {queue_name} to exchange {self._MAINEXCHANGE}!")
        self.start_consuming(queue_name=queue_name)
        
    def start_consuming(self, queue_name):
        
        # Start consuming messages from our various queues
        self._consuming = True
        
        slog.info(f"Starting to consume on {queue_name}.")
        if queue_name == self._ch_send_msg_q:
            self._ch_send_msg_q_tag = self._channel.basic_consume(queue=self._ch_send_msg_q, on_message_callback=self.ch_send_msg_consumer, auto_ack=True)
        elif queue_name == self._ch_msg_getstate_q:
            self._ch_msg_getstate_q_tag = self._channel.basic_consume(queue=self._ch_msg_getstate_q, on_message_callback=self.ch_msg_getstate_consumer, auto_ack=True)
        elif queue_name == self._ch_recv_msg_q:
            self._ch_recv_msg_q_tag = self._channel.basic_consume(queue=self._ch_recv_msg_q, on_message_callback=self.ch_recv_msg_consumer, auto_ack=True)
        elif queue_name == self._oh_recv_msg_q:
            self._oh_recv_msg_q_tag = self._channel.basic_consume(queue=self._oh_recv_msg_q, on_message_callback=self.oh_recv_msg_consumer, auto_ack=False)
        elif queue_name == self._oh_deliver_msg_q:
            self._oh_deliver_msg_q_tag = self._channel.basic_consume(queue=self._oh_deliver_msg_q, on_message_callback=self.oh_deliver_msg_consumer, auto_ack=False)
        else:
            slog.error("Queue not found.")
            self.stop()
            
    def on_cancelok(self, _unused_frame, queue_name):
        slog.info(f"RabbitMQ successfully closed queue {queue_name}")
        
    def run(self):
        """
        Set up the various request consumers that clients (both app and sim) 
        can send messages to. Combines a connect and start functionality into one function
        """
        
        # Event loop that handles both errors from Pika and others
        # while not self._stopping:
        try:
            # Create our base connection (SelectConnection vs BlockingConnection because we don't want our callbacks to block)
            slog.info("Attempting to establish connection...")
            self._connection = pika.SelectConnection(pika.URLParameters(url=self._rabbiturl), on_open_callback=self.on_connection_open, on_open_error_callback=self.on_connection_open_error, on_close_callback=self.on_connection_closed)
            
            slog.info("Server is starting IOLoop.")
            self._connection.ioloop.start()
            
            
        # Don't recover if connection was closed by broker or a client
        except pika.exceptions.ConnectionClosedByBroker:
            slog.error("Connection closed by broker, stopping...")
            self.stop()
            # break
        except pika.exceptions.ConnectionClosedByClient:
            slog.error("Connection closed by client, stopping...")
            self.stop()
            # break
        # Don't recover on channel errors
        except pika.exceptions.AMQPChannelError:
            slog.error("AQMP channel error, stopping...")
            self.stop()
            # break
        # Recover on all other connection errors
        except pika.exceptions.AMQPConnectionError:
            slog.warning("AQMP connection error, attempting to continue...")
            # continue
        except KeyboardInterrupt:
            slog.warning("Keyboard interrupt detected, stopping...")
            self.stop()
            # break
        
        
        
        """
        # Set, bind, and set to listen ports.
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((nsbp.HOST, nsbp.PORT))
        sock.listen()
        slog.info(f"Listening on port {nsbp.PORT}...")
        sock.setblocking(False)
        # Register the socket to be monitored.
        self.sel.register(sock, selectors.EVENT_READ, data=None)
        # Event loop.
        while True:
            try:
                # If tracking network metrics and message limit is set, raise KeyboardInterrupt.
                if self.track_network_metrics and self.tracker.getNumberOfMessages() >= self.message_limit:
                    raise KeyboardInterrupt
                events = self.sel.select(timeout=None)
                for key, mask in events:
                    if key.data is None:

                        # client = Client(key.fileobj, self.sel, self)
                        # self.clients[client.hash] = client
                        self.accept_wrapper(key.fileobj)
                    else:
                        # hash_ = hash(key.data.addr)
                        # client = self.clients[hash_]
                        # client.service_connection(key, mask)
                        try:
                            self.service_connection(key, mask)
                        except Exception as e:
                            # slog.error(f"Caught exception, terminating connection:\n{e}")
                            # self.unregister_and_close(key.fileobj)
                            raise(e)
            except KeyboardInterrupt:
                slog.critical("Caught keyboard interrupt, ending server process.")
                # Collect tracking information.
                if self.track_network_metrics:
                    slog.info("Collecting tracking information...")
                    complete_incomplete = self.tracker.cleanIncompleteEntries()
                    slog.info(f"\t{complete_incomplete[0]} completed messages, {complete_incomplete[1]} incomplete messages.")
                    slog.info(f"\tAverage latency: {self.tracker.getAverageLatency()} seconds")
                    slog.info(f"\tTotal throughput: {self.tracker.getTotalThroughput()} bytes/second")
                sys.exit(0)
            except ConnectionResetError:
                slog.error("Client connection reset.")
                self.unregister_and_close(key.fileobj)
            except Exception as e:
                slog.error(f"Caught exception:\n{e}")
                raise
        # finally:
        #     #self.sel.close()
        #     unregister_and_close(sock)
        
        """
    
    
    def stop(self):
        """
        This function will handle closing the connection, closing the channel, clearing queues
        """
        slog.info(f"Stopping the server consumer")
        self._stopping = True
        self._consuming = False
        if self._channel is not None:
            
            slog.info("Clearing existing queues on server")
            cb = functools.partial(self.on_cancelok, queue_name=self._ch_send_msg_q)
            self._channel.basic_cancel(self._ch_send_msg_q_tag)
            cb = functools.partial(self.on_cancelok, queue_name=self._ch_msg_getstate_q)
            self._channel.basic_cancel(self._ch_msg_getstate_q_tag, cb)
            cb = functools.partial(self.on_cancelok, queue_name=self._ch_recv_msg_q)
            self._channel.basic_cancel(self._ch_recv_msg_q_tag, cb)
            cb = functools.partial(self.on_cancelok, queue_name=self._oh_recv_msg_q)
            self._channel.basic_cancel(self._oh_recv_msg_q_tag, cb)
            cb = functools.partial(self.on_cancelok, queue_name=self._oh_deliver_msg_q)
            self._channel.basic_cancel(self._oh_deliver_msg_q_tag, cb)
            
            slog.info(f"Closing channel on server consumer")
            self._channel.close()
            
        if self._connection is not None:
            slog.info(f"Closing connection on server consumer")
            # We want to close the connection because when the server is stopped, we don't want 
            # clients still sending messages on a connection we aren't working with
            self._connection.close()
        
    def ch_send_msg_consumer(self, ch, method, props, body):
        
        """
        Per https://github.com/pika/pika/blob/main/examples/asynchronous_consumer_example.py#L300
        Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.
        """
        
        """
        CH_SEND_MSG: Application is requesting to send a message through the network.
        """
        
        data = json.loads(body)
        header = data["header"]
        message = data["body"]
        
        # Because messages are sent as JSON, take the header and convert it into a Header object
        header = self.objectifyHeader(header)
        
        # Take the message, look up the client, move it into their transmit queue, send an ack, return

        # Check for the header type (redundant check, will only be here if routing key was CH_SEND_MSG)
        if not header or not header.type:
            slog.warning("Server received a 'CH_SEND_MSG' message from an AppClient with no attached header")
            return
        
        # Find our client object from the header
        client = self.clientLookup(header)
        if not client:
            # Raise error and print out header information
            slog.error(f"Client not found.\nHeader information:\n{header}")
            raise ClientNotFoundError(f"Client not found.\nHeader information:\n{header}")
        
        slog.info(f"CH_SEND_MSG received from {client.clientIp}/{client.nodeId}...")
        
        # Create a new message and store it.
        print("Message is ", message)
        (rc, msg) = client.createNewMessage(header, message)
        
        msgid = 0
        if msg: msgid = msg.id
        
        # Start tracking the message.
        # Need to make sure we are using the correct message map
        # if self.track_network_metrics:
        #     self.tracker.addMessage(msgid, header.srcid, header.dstid, header.dataLen)
            
        # Send the client ID back to the AppClient client
        ch.basic_publish(exchange="", routing_key=props.reply_to, properties=pika.BasicProperties(correlation_id=props.correlation_id),body=str(msgid))
        
        
        
    def ch_msg_getstate_consumer(self, ch, method, props, body):
        
        """
        CH_MSG_GETSTATE: Application is requesting the state of a message.
        """
        
        data = json.loads(body)
        header = data["header"]
        message = data["body"]
        
        # Because messages are sent as JSON, take the header and convert it into a Header object
        header = self.objectifyHeader(header)
        
        # Take the message, look up the client, get the message state, send it back to the Client

        # Check for the header type (redundant check, will only be here if routing key was CH_MSG_GETSTATE)
        if not header or not header.type:
            slog.warning("Server received a 'CH_MSG_GETSTATE' message from an AppClient with no attached header")
            return
        
        # Find our client object from the header
        client = self.clientLookup(header)
        if not client:
            # Raise error and print out header information
            slog.error(f"Client not found.\nHeader information:\n{header}")
            raise ClientNotFoundError(f"Client not found.\nHeader information:\n{header}")
        
        
        slog.info(f"CH_MSG_GETSTATE received from {client.clientIp}/{client.nodeId}...")
        
        # Get the state of the message via the client
        status = str(client.getMsgState(header.msgid))
        if status == None:
            slog.warning(f"msg id {header.msg} not found as valid mapping.")
        
        slog.info(f"Status: {status}")
        
        # Send a reply back to client and ack the original appclient message
        ch.basic_publish(exchange="", routing_key=props.reply_to, properties=pika.BasicProperties(correlation_id=props.correlation_id),body=status)
        
    def ch_recv_msg_consumer(self, ch, method, props, body):
        
        """
        CH_RECV_MSG: Application is requesting to receive a message.
        """
        
        data = json.loads(body)
        header = data["header"]
        
        # Because messages are sent as JSON, take the header and convert it into a Header object
        header = self.objectifyHeader(header)
        
        # Take the message, look up the client, move it into their transmit queue, send an ack, return

        # Check for the header type (redundant check, will only be here if routing key was CH_RECV_MSG)
        if not header or not header.type:
            slog.warning("Server received a 'CH_RECV_MSG' message from an AppClient with no attached header")
            return
        
        # Find our client object from the header
        client = self.clientLookup(header)
        if not client:
            # Raise error and print out header information
            slog.error(f"Client not found.\nHeader information:\n{header}")
            raise ClientNotFoundError(f"Client not found.\nHeader information:\n{header}")
        
        
        slog.info(f"CH_RECV_MSG received from {client.clientIp}/{client.nodeId}...")
        
        ####
        # message = client.getChRespMsg()
        returnMsg = {
            "header": {
                "type": nsbp.MSG_TYPES.CH_RESP_MSG,
                "dataLen": 5,
                "srcid": header.srcid,
                "dstid": header.dstid,
                "msgid": header.msgid
            },
            "body": "hello"
        }
        message = json.dumps(returnMsg)
        
        # Send a reply back to client and ack the original appclient message
        ch.basic_publish(exchange="", routing_key=props.reply_to, properties=pika.BasicProperties(correlation_id=props.correlation_id),body=message)
        
    def oh_recv_msg_consumer(self, ch, method, props, body):
        
        """
        OH_RECV_MSG: Simulator is requesting to pick up a message.
        """
        
        print("Consuming sim client messages")
        
        # Send a reply back to client and ack the original sim client message
        ch.basic_publish(exchange="", routing_key=props.reply_to, properties=pika.BasicProperties(correlation_id=props.correlation_id),body="Some body")
        ch.basic_ack(delivery_tag=props.delivery_tag)
        
    def oh_deliver_msg_consumer(self, ch, method, props, body):
        
        """
        OH_DELIVER_MSG: Simulator is notifying delivery of a message.
        """
        
        print("Consuming sim client messages")
        
        # Take the message, find the appropriate client, move the message from that client's transceive queue to its receive queue, send an ack, return
        ch.basic_ack(delivery_tag=method.delivery_tag)
    
    # Messages sent as JSON, create and return a Header object from JSON
    def objectifyHeader(self, header) -> Header: 
        r = Header()
        r.type = header["type"]
        r.dataLen = header["dataLen"]
        r.srcid = header["srcid"]
        r.dstid = header["dstid"]
        r.msgid = header["msgid"]
        return r
        

    """
    Functions to facilitate connections between multiple clients and the server. Based on a guide
    provided in https://realpython.com/python-sockets/#handling-multiple-connections.
    """
    def accept_wrapper(self, sock):
        """
        Accepts and registers new connection.
        """
        conn, addr = sock.accept()
        self.hash = hash(addr)
        slog.debug(f"Accepted connection from {self.hash} {addr} {conn}")
        # Disable blocking.
        conn.setblocking(False)
        # Create data object to monitor for read and write availability.
        data = types.SimpleNamespace(addr=addr, inb=b"", outb=b"")
        #events = selectors.EVENT_READ | selectors.EVENT_WRITE
        events = selectors.EVENT_READ
        # Register with selector.
        self.sel.register(conn, events, data=data)

    def service_connection(self, key:selectors.SelectorKey, mask):
        """
        Services the existing connection and calls to unregister upon completion.
        """
        sock = key.fileobj
        data = key.data
        pktData = ''.encode()
        header = None
        # Check for reads or writes.
        if mask & selectors.EVENT_READ:
            # At event, it should be ready for read. Read the header first
            header, pktData = self.recvData(sock)
        # If header is None, then the connection is closed.
        ### Debugging for protobuf: here shows that header and header.type exist and does not return here, unlike in protobuf version
        if not header or not header.type:
            return
        # Use the header to look up the client to return a Client object.
        client = self.clientLookup(header)
        # If the client is not found, then the connection is closed.
        if not client:
            # Raise error and print out header information
            raise ClientNotFoundError(f"Client not found.\nHeader information:\n{header}")
        # If the client is found, then service the connection.
        slog.debug(f"Service connection for client {client.clientIp}/{client.nodeId}")
        
        # Client contains the client that sent the message to the server
        
        if header.type == nsbp.MSG_TYPES.CH_SEND_MSG:
            """
            CH_SEND_MSG: Application is requesting to send a message through the network.
            """
            # assert(header.dataLen == nsbp.CH_SEND_MSG_SIZE)
            slog.info(f"CH_SEND_MSG received from {client.clientIp}/{client.nodeId}...")
            # Create a new message and store it.
            (rc, msg) = client.createNewMessage(header, pktData)
            if msg: msgid = msg.id
            else: msgid = 0
            # Start tracking the message.
            if self.track_network_metrics:
                self.tracker.addMessage(msgid, header.srcid, header.dstid, header.dataLen)
            # Send the ack back to the application.
            client.sendChMsgAck(sock, header, msgid, rc)
            slog.info(f"\tAck'ed message from {client.clientIp}/{client.nodeId}.")
        elif header.type == nsbp.MSG_TYPES.CH_MSG_GETSTATE:
            """
            CH_MSG_GETSTATE: Application is requesting the state of a message.
            """
            slog.info(f"CH_MSG_GETSTATE received from {client.clientIp}/{client.nodeId}...")
            # Check that the packet data is the correct size.
            assert(nsbp.CH_MSG_GETSTATE_SIZE == len(pktData))
            # Return the message state to the application via socket.
            client.sendChMsgState(sock, pktData)
            slog.info(f"\tSent message state to {client.clientIp}/{client.nodeId}.")
        elif (header.type == nsbp.MSG_TYPES.CH_RECV_MSG):
            """
            CH_RECV_MSG: Application is requesting to receive a message.
            """
            slog.debug(f"CH_RECV_MSG received from {client.clientIp}/{client.nodeId}...")
            # Check that the packet data is the correct size.
            assert(nsbp.CH_RECV_MSG_SIZE == len(pktData))
            # Return the message to the application via socket.
            client.sendChRespMsg(sock)
        elif (header.type == nsbp.MSG_TYPES.OH_RECV_MSG):
            """
            OH_RECV_MSG: Simulator is requesting to pick up a message.
            """
            # Check that the packet data is the correct size.
            assert(nsbp.OH_RECV_MSG_SIZE == len(pktData))
            # Return the message to the simulator via socket.
            client.sendOhRespMsg(sock)
        elif (header.type == nsbp.MSG_TYPES.OH_DELIVER_MSG):
            """
            OH_DELIVER_MSG: Simulator is notifying delivery of a message.
            """
            slog.info(f"OH_DELIVER_MSG received from {client.clientIp}/{client.nodeId}...")
            # Get client object for the source node.
            srcClient = self.node_reference[header.srcid]
            slog.info(f"\tMessage delivered from {srcClient.clientIp}/{srcClient.nodeId} to {client.clientIp}/{client.nodeId}.")
            # Move the message from the source transit queue to the dest receive queue.
            rc = client.transitq2rxq(srcClient, header, pktData)
            # Send the ack back to the simulator.
            client.sendOhDeliveryMsgAck(sock, header, rc)
            slog.info(f"\tAck'ed message delivery for {client.clientIp}/{client.nodeId}.")
            # Stop tracking the message.
            if self.track_network_metrics:
                self.tracker.endMessage(header.msgid)

    def unregister_and_close(self, sock:socket.socket):
        """
        Unregisters and closes the connection, called at the end of service.
        """
        slog.debug("Closing connection...")
        # Unregister the connection.
        try:
            self.sel.unregister(sock)
        except Exception as e:
            slog.error(f"Socket could not be unregistered:\n{e}")
        # Close the connection.
        try:
            sock.close()
        except OSError as e:
            slog.error(f"Socket could not close:\n{e}")

    def recvData (self, sock):
        """
        Receive data from the socket. Returns a tuple of the header and the data.
        """
        # At event, it should be ready for read. Read the header first.
        recv_data = sock.recv(nsbp.CH_HEADER_SIZE)
        h = Header()
        data = ''.encode()
        # Check if any data was received.
        if recv_data:
            # Unpack the header.
            h.type, h.dataLen, h.srcid, h.dstid = struct.unpack(nsbp.CH_HEADER_FORMAT, recv_data)
            slog.debug("Received type: %d len %d, src=%d dst=%d" %
                (h.type, h.dataLen, h.srcid, h.dstid))
            _data = ''.encode()
            # Check the scope (CH vs. OH) of the incoming data.
            if h.type > nsbp.MSG_TYPES.CH_MSGS_END:
                h.msgid, = struct.unpack("="+nsbp.MSGID_FORMAT, sock.recv(nsbp.MSGID_SIZE))
                slog.debug("Message ID: %s" % h.msgid)
            # Read the data in its entirety.
            while (len(data) < h.dataLen):
                _data = sock.recv(h.dataLen - len(_data))
                data += _data
            # while True:
            #     _data = sock.recv(1024)
            #     if not _data: break
            #     data += _data
        else:
            slog.debug(f"Closing connection to {sock}.")
            self.unregister_and_close(sock)
        # Return the header and the data.
        return (h, data)
    def init_clients(self, filename, nodeid_type="int"):
        """
        Initialize all the clients.
        """
        self.ip_reference = {}
        self.node_reference = {}
        with open(filename, 'r') as f:
            for line in f:
                line = line.strip()
                ip, nodeid = line.split(":")
                # IP string -> IPV4
                ip = socket.gethostbyname(ip)
                # IPV4 -> uint32. The return is a tuple. hence "alias,"
                ip, = struct.unpack("!I", socket.inet_pton(socket.AF_INET, ip))
                # Convert the nodeid depending on the type.
                if nodeid_type == "int":
                    # Convert node to int.
                    nodeid = int(nodeid)
                elif nodeid_type == "ip":
                    # IP string -> IPV4
                    nodeid = socket.gethostbyname(nodeid)
                    # Convert IP to uint32.
                    nodeid, = struct.unpack("!I", socket.inet_pton(socket.AF_INET, nodeid))
                print (ip, nodeid)
                client = Client(ip, nodeid, self)
                self.ip_reference[ip] = client
                self.node_reference[nodeid] = client
        # Log the references for info, neatly.
        slog.info("IP references:")
        for ip, client in self.ip_reference.items():
            slog.info(f"\t{ip} -> {client}")
        slog.info("Node references:")
        for nodeid, client in self.node_reference.items():
            slog.info(f"\t{nodeid} -> {client}")
        # Thank you.
        slog.info("Client initialization complete.")

    def clientLookup(self, header):
        """
        Lookup the client object based on the header.
        """
        clog.debug(f"Client lookup: srcid {header.srcid} dstid {header.dstid}")
        if header.type == nsbp.MSG_TYPES.OH_DELIVER_MSG:
            if header.dstid in self.node_reference:
                return self.node_reference[header.dstid]
        elif header.type > nsbp.MSG_TYPES.CH_MSGS_END:
            if header.srcid in self.node_reference:
                return self.node_reference[header.srcid]
        elif header.type == nsbp.MSG_TYPES.CH_RECV_MSG:
            if header.dstid in self.ip_reference:
                return self.ip_reference[header.dstid]
        else:
            if header.srcid in self.ip_reference:
                return self.ip_reference[header.srcid]
        return None # Default


if __name__ == '__main__':
    import argparse
    import os
    # Use argparse to get the map file name and to enable network metric tracking.
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--map_file_name", help="The name of the alias map file.")
    parser.add_argument("-n", "--network_metrics", help="Enable network metrics tracking.", action="store_true", default=False)
    # Argument for message limit, where server ends after receiving a certain number of messages.
    parser.add_argument("-l", "--message_limit", help="The number of messages to receive before ending.", type=int, default=0)
    args = parser.parse_args()
    map_file_name = args.map_file_name
    network_metrics = args.network_metrics
    # Run the server.
    server = NSBServer(map_file_name, network_metrics, args.message_limit)
    # Announce PID.
    slog.critical(f"Server PID: {os.getpid()}")
    server.run()
