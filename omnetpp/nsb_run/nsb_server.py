import sys
from os import remove
from select import select
import socket
import selectors
import types
import copy

from ctypes import *
import nsb_payload as nsbp
import struct

import logging

logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s: %(message)s',)
# Client logger.
clog = logging.getLogger('(client)')
clog.setLevel(logging.INFO)
# Server logger.
slog = logging.getLogger('(server)')
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
        # Create a queue for each message state, including a message map.
        self.msgmap = dict()
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
            assert (msg.id not in self.msgmap)
            assert (msg.id not in self.txq)
            # Add message to map and queue.
            self.msgmap[msg.id] = msg
            self.txq.append(msg.id)
            clog.debug(f"\tMessage {msg.id} added.")
        elif dstq == nsbp.MSG_Q.MSGQ_TRANSIT:
            # Transit queue.
            clog.debug(f"\tAdding message {msg.id} to transit queue...")
            # Check that message is in the transmit queue, and not in transit.
            assert (msg.q == nsbp.MSG_Q.MSGQ_TX)
            assert(msg.id in self.msgmap)
            assert (msg.id not in self.transitq)
            # Add to transit queue.
            self.transitq.append(msg.id)
            clog.debug(f"\tMessage {msg.id} added.")
        elif dstq == nsbp.MSG_Q.MSGQ_RX:
            # Receive queue.
            clog.debug(f"\tHandling message {msg.id} in RX queue...")
            # Check that message state is valid.
            if msg.id in self.msgmap or \
              msg.id in self.rxq:
                clog.debug(f"\tMessage {msg.id} already delivered.")
                return nsbp.ERROR_CODES.MESSAGE_ALREADY_DELIVERED
            # Check that message is in transit.
            if msg.q != nsbp.MSG_Q.MSGQ_TRANSIT:
                clog.debug(f"\tMessage {msg.id} in wrong state.")
                return nsbp.ERROR_CODES.MESSAGE_IN_WRONG_STATE
            # Add to receive queue.
            self.msgmap[msg.id] = msg
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
        if msgid not in self.msgmap:
            return nsbp.MSG_STATE.MSG_STATE_NOTFOUND
        # Check the message queue state.
        if self.msgmap[msgid].q == nsbp.MSG_Q.MSGQ_TX:
            assert(msgid in self.txq)
            return nsbp.MSG_STATE.MSG_STATE_QUEUED
        elif self.msgmap[msgid].q == nsbp.MSG_Q.MSGQ_TRANSIT:
            assert(msgid in self.transitq)
            return nsbp.MSG_STATE.MSG_STATE_INTRANSIT
        elif self.msgmap[msgid].q == nsbp.MSG_Q.MSGQ_RX:
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
        # Create message.
        msg = Msg(get_msg_id(), header, pktData)
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
        clog.debug(f"Handling app request for received messages...")
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
            msg = self.msgmap[msgid]
            fmt = "%s%ss" % (nsbp.CH_HEADER_FORMAT, len(msg.data))
            # if DEBUG: print ("FORMAT %s" % fmt)
            retbuf = struct.pack(fmt, nsbp.MSG_TYPES.CH_RESP_MSG,
                len(msg.data), msg.header.srcid, msg.header.dstid, msg.data)
            # Remove message from message map.
            self.msgmap.pop(msgid)
            clog.info(f"\tMessage {msgid} retrieved and removed from message map.")
        clog.debug(f"\tResponse buffer length: {len(retbuf)}")
        sock.sendall(retbuf)
        clog.debug(f"\tResponse sent.")

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
            msg = self.msgmap[msgid]
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
        if header.msgid not in srcClient.msgmap:
            return nsbp.ERROR_CODES.MESSAGE_NOT_FOUND
        if header.msgid not in srcClient.transitq:
            return nsbp.ERROR_CODES.MESSAGE_IN_WRONG_QUEUE
        # Remove message from sender's transit queue and message map.
        index = srcClient.transitq.index(header.msgid)
        msgid = srcClient.transitq.pop(index)
        msg = srcClient.msgmap.pop(msgid)
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
        

class NSBServer(object):
    """
    NSBServer: Initialization, Helper, and Runtime Methods
    """
    def __init__(self, map_file_name):
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
        slog.info(f"Server initialized.")
    def run(self):
        """
        Starts listening for connections and starts the main event loop.
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
                            slog.error(f"Caught exception, terminating connection:\n{e}")
                            self.unregister_and_close(key.fileobj)
            except KeyboardInterrupt:
                slog.critical("Caught keyboard interrupt, exiting...")
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
    # Use argparse to get the map file name.
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--map_file_name", help="The name of the alias map file.")
    args = parser.parse_args()
    map_file_name = args.map_file_name
    # Run the server.
    server = NSBServer(map_file_name)
    server.run()
