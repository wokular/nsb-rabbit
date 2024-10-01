"""
Send a simple set of byte messages to the server.
"""

import socket
import nsb_payload as nsbp
import struct
import logging
import random
import os
import messaging_pb2
import asyncio
from aioconsole import ainput

# Set up logging for server.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',)
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
    def __init__(self, server_addr=NSB_SERVER_ADDR):
        """
        Initialize the client by creating and maintaining a connection to the server.
        """
        # Create a socket.
        self.sock = socket.socket()
        # Connect to the server.
        self.sock.connect((server_addr, nsbp.PORT))
        # Get local IP address.
        self.local_ip = socket.gethostbyname(socket.gethostname())

    def __del__(self):
        """
        Close the connection to the server.
        """
        # Close the socket.
        self.sock.close()
        
    def send(self, src_id, dest_id, message):
        message_length = len(message)

        # Prepare the protobuf message
        send_msg = messaging_pb2.SendMessage()
        send_msg.header.type = nsbp.MSG_TYPES.CH_SEND_MSG
        send_msg.header.data_len = message_length
        send_msg.header.srcid = ip_to_int(src_id)
        send_msg.header.dstid = ip_to_int(dest_id)
        send_msg.data = message

        # Serialize and send the message
        self.sock.send(send_msg.SerializeToString())

        # Receive and parse the reply
        reply = self.sock.recv(1024)
        reply_msg = messaging_pb2.AckMessage()
        reply_msg.ParseFromString(reply)

        if reply_msg.header.type == nsbp.MSG_TYPES.CH_SEND_MSG_ACK:
            if reply_msg.return_code == nsbp.ERROR_CODES.SUCCESS:
                return 0
            else:
                return 1

    def receive(self, dest_id):
    # Prepare the protobuf message
        recv_msg = messaging_pb2.ReceiveMessage()
        recv_msg.header.type = nsbp.MSG_TYPES.CH_RECV_MSG
        recv_msg.header.data_len = 0
        recv_msg.header.srcid = 0  # Placeholder
        recv_msg.header.dstid = ip_to_int(dest_id)

        # Serialize and send the message
        self.sock.send(recv_msg.SerializeToString())

        # Receive and parse the reply
        reply = self.sock.recv(1024)
        reply_msg = messaging_pb2.MessageReply()
        reply_msg.ParseFromString(reply)

        if reply_msg.header.type == nsbp.MSG_TYPES.CH_RESP_MSG:
            src_addr = int_to_ip(reply_msg.header.srcid)
            dest_addr = int_to_ip(reply_msg.header.dstid)
            return src_addr, dest_addr, reply_msg.data


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
            rlog.debug(f"\t> {alias}")
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
    
