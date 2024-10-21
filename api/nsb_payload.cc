#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
// #include <boost/any.hpp>
#include <algorithm>
#include <assert.h>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <fstream>
#include <iostream>
#include <pthread.h>
#include <thread>
#include <vector>

#include "nsb_payload.h"

// https://github.com/alanxz/rabbitmq-c

/*
nsb_payload.cc / nsb_payload.h:

    🐳 CONTAINERS  NODE         _______________    HOST   ________________
      nsb_client   [0]  <----> |               |         |     OMNET++    |
      nsb_client   [1]  <----> | nsb_server.py |         | nsb_payload.cc |
          ...                  |               |  <----> |  sendPayload() |
      nsb_client   [n]  <----> |_______________|         |________________|
*/

#define DEBUG 0
#define ACTIVE_DEBUG 1

// #define POLLING_INTERVAL 100ms

// TRANSMISSION FUNCTIONS.

const char *_OH_MSG_TYPES_STR[] = {
    "OH_DELIVER_MSG",  "OH_DELIVER_MSG_ACK", "OH_RECV_MSG", "OH_RESP_MSG",
    "OH_MSG_GETSTATE", "OH_MSG_STATE",       "OH_MSG_DUMMY"};

const char *_ERROR_CODES[] = {"SUCCESS",
                              "MESSAGE_NOT_FOUND",
                              "MESSAGE_ALREADY_DELIVERED",
                              "MESSAGE_IN_WRONG_QUEUE",
                              "MESSAGE_TO_SELF",
                              "OH_ERR_SOCK_CREATE",
                              "OH_ERR_INVALID_ADDR",
                              "OH_ERR_CONN_FAIL"};

/*
HELPER FUNCTIONS.

These functions will help set up the connection to the NSB server, send
messages, and receive messages. These will be used by the following classes and
functions.
*/

int getConnection(void)
/*
Establishes a connection to the NSB server. Returns the socket integer.
*/
{
  if (DEBUG)
    printf("Connecting to the socket...");
  // Used code from: https://www.geeksforgeeks.org/socket-programming-cc/
  int sock = 0;
  struct sockaddr_in serv_addr;
  //    char hello[] = "exit";
  if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    printf("\n Socket creation error \n");
    return -OH_ERR_SOCK_CREATE;
  }

  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(O_SERVER_PORT);

  // Convert IPv4 and IPv6 addresses from text to binary form.
  if (inet_pton(AF_INET, O_SERVER_ADDRESS, &serv_addr.sin_addr) <= 0) {
    printf("\nInvalid address/ Address not supported \n");
    return -OH_ERR_INVALID_ADDR;
  }

  if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    printf("\nConnection Failed \n");
    return -OH_ERR_CONN_FAIL;
  }
  if (DEBUG)
    printf("\n ... Connected!\n");
  return sock;
}

static void nsbSendMsg(int sock, void *msg, uint32_t msgsize)
/*
Sends a message over the previously established socket.
*/
{
  if (write(sock, msg, msgsize) < 0) {
    printf("Can't send message.\n");
    close(sock);
    exit(1);
  }
  //    printf("Message sent (%d bytes).\n", msgsize);
  return;
}

static void dumpHeader(oh_header_t *oh)
/*
For debugging purposes, prints the header of a NSB message.
*/
{
  return;

  printf("HEADER - Type %s (%d) Len %u srcid %u dstid %u msgid %u\n",
         OH_MSG_TYPES_STR(oh->type), oh->type, oh->len, oh->srcid, oh->dstid,
         oh->msgid);
}

static int recv_header(int sock, oh_header_t *h, size_t sz)
/*
Receives just the header of a NSB message.
*/
{
  //    printf("READING HEADER -> ");
  int nread = read(sock, h, sz);
  //    printf("HEADER %d bytes (sz %ld)\n", nread, sz);
  dumpHeader(h);
  return nread;
}

/*
LOW LEVEL FUNCTIONS (LEGACY).

These functions are one-directional and are used to send requests and receive
responses from the NSB server. They are good to use when you are looking for
non-blocking implementations, but will require that you implement both parts of
each transaction.
*/

/*
Sim wants to pick up a message
*/
int send_oh_recv_msg(int sock, uint32_t srcid) {
  oh_header_t header;

  header.type = OH_RECV_MSG, header.len = 0, header.dstid = (uint32_t)-1,
  header.msgid = (uint32_t)-1, header.srcid = srcid;

  nsbSendMsg(sock, &header, sizeof(oh_header_t));
  return 0;
}

/*
Sim is receiving the message it previously requested to receive
*/
void *recv_oh_resp_msg(int sock, oh_header_t *header) {
  typedef struct {
    uint32_t id;
    double temp;
    uint32_t counter;
  } oh_resp_msg_t;

  void *data = NULL;

  int nread = recv_header(sock, header, sizeof(oh_header_t));
  assert(header->type == OH_RESP_MSG);
  if (header->len > 0) {
    data = malloc(header->len);
    nread = read(sock, data, header->len);
    assert(header->len == nread);
  }
  return data;
}

/*
Sim is notifying server it sent the message that went through its network
*/
int send_oh_deliver_msg(int sock, uint32_t len, uint32_t srcid, uint32_t dstid,
                        uint32_t msgid, void *data) {
  void *pkt = malloc(sizeof(oh_header_t) + len);

  oh_header_t *header = (oh_header_t *)pkt;

  header->type = OH_DELIVER_MSG, header->len = len, header->msgid = msgid,
  header->srcid = srcid, header->dstid = dstid;

  memcpy((char *)pkt + sizeof(oh_header_t), data, len);
  nsbSendMsg(sock, pkt, sizeof(oh_header_t) + len);

  return 0;
}

/*
Sim is receiving server's ack of its previous "message delivered to server"
notification message it had sent
*/
oh_deliver_msg_ack_t *recv_oh_deliver_msg_ack(int sock) {
  oh_header_t header;

  oh_deliver_msg_ack_t *oh_ack =
      (oh_deliver_msg_ack_t *)malloc(sizeof(oh_deliver_msg_ack_t));

  int nread = recv_header(sock, &header, sizeof(oh_header_t));
  assert(header.type == OH_DELIVER_MSG_ACK);
  assert(header.len == sizeof(oh_deliver_msg_ack_t));

  nread = read(sock, oh_ack, sizeof(oh_deliver_msg_ack_t));
  printf("Received returnCode=%s (%d)\n", ERROR_CODE_STR(oh_ack->returnCode),
         oh_ack->returnCode);

  return oh_ack;
}

/*
HIGH-LEVEL NSB FUNCTIONS.

These functions provide high-level access to the NSB server, and will take care
of contacting the server, sending the appropriate messages, and receiving the
appropriate responses.
*/

NSBConnector::NSBConnector() {
  // Set up socket.
  if (ACTIVE_DEBUG)
    printf("Setting up socket.\n");
  sock = getConnection();
  if (ACTIVE_DEBUG)
    printf("Connection set.\n\n");
  // printf("Socket:");
}

NSBConnector::~NSBConnector() {
  // Close socket.
  if (DEBUG)
    printf("Deconstructor called. Closing socket.\n");
  close(sock);
}
nsbHeaderAndData_t
NSBConnector::receiveMessageFromNsbServer(std::string sourceAddress) {
  // Set up socket.
  // printf("Setting up socket\n");
  // int sock = getConnection();
  // printf("Socket:");

  // Convert string IP address to uint32_t.
  // printf("Source address: %s", sourceAddress.c_str());
  uint32_t sourceAddress_u32 = htonl(inet_addr(sourceAddress.c_str()));
  // printf("Source address (uint32_t): %u", sourceAddress_u32);

  /* SEND REQUEST FOR MESSAGE. */
  // Create header.
  oh_header_t header;
  header.type = OH_RECV_MSG, header.len = 0, header.dstid = (uint32_t)-1,
  header.msgid = (uint32_t)-1, header.srcid = sourceAddress_u32;
  // This message has no additonal data, so we can send the header directly.
  if (DEBUG)
    printf("Sending request for message.\n");
  nsbSendMsg(sock, &header, sizeof(oh_header_t));
  // Print out header bytes.
  if (DEBUG) {
    printf("Header bytes: ");
    for (int i = 0; i < sizeof(oh_header_t); i++) {
      printf("%02x ", ((uint8_t *)&header)[i]);
    }
    printf("\n");
  }

  /* RECEIVE RESPONSE FROM SERVER. */
  // Receive header.
  int numBytesRead = recv_header(sock, &header, sizeof(oh_header_t));
  // Assert the response is of the correct type.
  assert(header.type == OH_RESP_MSG);
  // If there is additional data, read it.
  if (header.len > 0) {
    void *data = malloc(header.len);
    numBytesRead = read(sock, data, header.len);
    assert(header.len == numBytesRead);
    /* SEND INFORMATION BACK. */
    // Return header and data in the nsbHeaderAndData struct.
    nsbHeaderAndData_t headerAndData;
    headerAndData.header = header;
    headerAndData.data = data;
    // close(sock);
    return headerAndData;
  }
  // If there is no additional data, return header and NULL.
  else {
    nsbHeaderAndData_t headerAndData;
    headerAndData.header = header;
    headerAndData.data = nullptr;
    // close(sock);
    return headerAndData;
  }
}

int NSBConnector::notifyDeliveryToNsbServer(int msgLen, uint32_t sourceAddress,
                                            uint32_t destinationAddress,
                                            int msgId, void *message) {
  // Set up socket.
  // int sock = getConnection();

  // Convert string IP addresses to uint32_t.
  //    uint32_t sourceAddress_u32 = htonl(inet_addr(sourceAddress.c_str()));
  //    uint32_t destinationAddress_u32 =
  //    htonl(inet_addr(destinationAddress.c_str()));

  void *pkt = malloc(sizeof(oh_header_t) + msgLen);

  oh_header_t *header = (oh_header_t *)pkt;

  /* SEND REQUEST FOR MESSAGE. */
  // Use header pointer to set header values.
  header->type = OH_DELIVER_MSG, header->len = msgLen,
  header->dstid = destinationAddress, header->msgid = msgId,
  header->srcid = sourceAddress;
  // Copy message into packet.
  memcpy((char *)pkt + sizeof(oh_header_t), message, msgLen);
  // This message does not have any additional data, so we can send the header
  // directly.
  nsbSendMsg(sock, header, sizeof(oh_header_t) + msgLen);
  // Print out header bytes.
  if (DEBUG) {
    printf("Header bytes: ");
    for (int i = 0; i < sizeof(oh_header_t) + msgLen; i++) {
      printf("%02x ", ((uint8_t *)header)[i]);
    }
    printf("\n");
  }

  /* RECEIVE RESPONSE FROM SERVER. */
  // Receive header.
  int numBytesRead = recv_header(sock, header, sizeof(oh_header_t));
  // Assert the response is of the correct type.
  assert(header->type == OH_DELIVER_MSG_ACK);
  // If there is additional data, read it.
  if (header->len > 0) {
    void *data = malloc(header->len);
    numBytesRead = read(sock, data, header->len);
    assert(header->len == numBytesRead);
    /* SEND INFORMATION BACK. */
    // Check if data indicates SUCCESS and return based on that.
    // First, convert void pointer data (1 byte unsigned) to int.
    int _data = *((uint8_t *)data);
    if (_data == SUCCESS) {
      // close(sock);
      return 0;
    } else {
      // close(sock);
      return 1;
    }
  } else {
    // close(sock);
    return 1;
  }
}

/* TESTING RECEIVE AND NOTIFY FUNCTION. */
// Define a function that receives a message and immediately notifies the server
// that it was delivered.
int receiveAndNotify(NSBConnector *connector, std::string address) {
  /* RECEIVE MESSAGE FORM SERVER */
  // Receive a message from the server.
  nsbHeaderAndData_t message = connector->receiveMessageFromNsbServer(address);
  // Print out the first 10 bytes of the message, followed by ellipses.
  int min = message.header.len < 10 ? message.header.len : 10;
  if (DEBUG)
    std::cout << "Message: " << std::string((char *)message.data, min) << "..."
              << std::endl;
  // Store information from the message header.
  int msgId = message.header.msgid;
  // Get source and destination addresses in host byte order.
  // std::string sourceAddress = std::string(inet_ntoa(*(struct in_addr
  // *)&message.header.srcid)); std::string destinationAddress =
  // std::string(inet_ntoa(*(struct in_addr *)&message.header.dstid));
  int msgLen = message.header.len;
  if (DEBUG) {
    // Print out the message header information.
    std::cout << "Message ID: " << msgId << std::endl;
    // std::cout << "Source Address: " << sourceAddress << std::endl;
    // std::cout << "Destination Address: " << destinationAddress << std::endl;
    std::cout << "Source Address: " << message.header.srcid << std::endl;
    std::cout << "Destination Address: " << message.header.dstid << std::endl;
    std::cout << "Message Length: " << msgLen << std::endl;
  }
  // Check for message to print information.
  if (msgLen > 0 && ACTIVE_DEBUG) {
    // Print out the message header information.
    std::cout << "Message ID: " << msgId << std::endl;
    // std::cout << "Source Address: " << sourceAddress << std::endl;
    // std::cout << "Destination Address: " << destinationAddress << std::endl;
    std::cout << "Source Address: " << message.header.srcid << std::endl;
    std::cout << "Destination Address: " << message.header.dstid << std::endl;
    std::cout << "Message Length: " << msgLen << std::endl;
  }

  // Do some network simming here
  // No need to send a message containing any data to the server:
  // any simulated messages are sent within the sim and arrive within the sim
  // All we need to do is tell the server that the message was successfully
  // delivered to the destination specifified in the original received message
  // (^above) and await server's ack of that delivery notification message

  /* NOTIFY DELIVERY TO SERVER */
  // before calling notifyDeliveryToNsbServer, check if there is any message
  if (msgLen == 0) {
    // since there is no message, print out error
    if (DEBUG)
      std::cout << "There is no message! No call to notify server" << std::endl;
    return -1;
  } else {
    // Notify the server that the message was delivered.
    if (ACTIVE_DEBUG)
      std::cout << "Notifying server that message was delivered." << std::endl;
    int result = connector->notifyDeliveryToNsbServer(
        msgLen, message.header.srcid, message.header.dstid, msgId,
        message.data);
    // Print out the result of the delivery notification.
    std::cout << "Delivery notification result: " << result << std::endl;
    return result;
  }
}

// Lambda function receiveNotifyThread that runs the exiswting receiveAndNotify
// function on a given address in a while loop. void
// receiveNotifyThread(std::string address){
//     while (true){
//         receiveAndNotify(address);
//     }
// };

/*
MAIN TEST FUNCTION.

The main function will be used to test the high-level functions offered here.
*/
int main(int argc, char *argv[]) {

/*
SINGLE MESSAGE TESTING.

This section of the main function will be used to test the functions by sending
and notifying the delivery of a single message from one address to another.
*/
#if 0
    // Receive a message from the server.
    nsbHeaderAndData_t message = receiveMessageFromNsbServer("10.0.0.4");
    // Print out the first 10 bytes of the message, followed by ellipses.
    int min = message.header.len < 10 ? message.header.len : 10;
    std::cout << "Message: " << std::string((char *)message.data, min) << "..." << std::endl;
    // Store information from the message header.
    int msgId = message.header.msgid;
    // Get source and destination addresses in host byte order.
    //std::string sourceAddress = std::string(inet_ntoa(*(struct in_addr *)&message.header.srcid));
    //std::string destinationAddress = std::string(inet_ntoa(*(struct in_addr *)&message.header.dstid));
    int msgLen = message.header.len;
    // Print out the message header information.
    std::cout << "Message ID: " << msgId << std::endl;
    //std::cout << "Source Address: " << sourceAddress << std::endl;
    //std::cout << "Destination Address: " << destinationAddress << std::endl;
    std::cout << "Source Address: " << message.header.srcid << std::endl;
    std::cout << "Destination Address: " << message.header.dstid << std::endl;
    std::cout << "Message Length: " << msgLen << std::endl;
    //before calling notifyDeliveryToNsbServer, check if there is any message
    if (msgLen==0){
        //since there is no message, print out error
        std::cout << "There is no message! No call to notify server" <<std::endl;
    }
    else{
        // Notify the server that the message was delivered.
        int result = notifyDeliveryToNsbServer(msgLen,message.header.srcid , message.header.dstid, msgId, message.data);
        // Print out the result of the delivery notification.
        std::cout << "Delivery notification result: " << result << std::endl;
    }
#endif

/*
GHOST SIMULATOR CLIENT.

This (much cooler) section of the main function will be used to run a ghost
network simulator client that will always pick up and immediately deliver
messages. This can be utilized to test the other modules, such as the
application client or the
*/
#if 1

  using namespace std::this_thread; // sleep_for, sleep_until
  // using namespace std::chrono_literals; // ns, us, ms, s, h, etc.
  using std::chrono::system_clock;

  // Create a list of IP addresses here (in this case, from 10.0.0.1
  // to 10.0.0.9)
  std::vector<std::string> addresses;
  std::vector<NSBConnector *> connectors;

  // Set filename from command line argument.
  std::string map_file_name = argv[1];
  // Read in addresses from map_file_name (only before the colon on each line).
  std::ifstream aliasmap(map_file_name);
  std::string line;
  while (std::getline(aliasmap, line)) {
    addresses.push_back(line.substr(0, line.find(":")));
    connectors.push_back(new NSBConnector());
  }

  while (true) {
    for (int i = 0; i < addresses.size(); i++) {
      receiveAndNotify(connectors[i], addresses[i]);
    }
    // Delay for POLLING_INTERVAL seconds.
    sleep_for(std::chrono::milliseconds(100));
  }

#endif
}
