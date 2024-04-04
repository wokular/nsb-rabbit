/*
nsb_payload.cc / nsb_payload.h:

    🐳 CONTAINERS  NODE         _______________    HOST   ________________
      nsb_client   [0]  <----> |               |         |     OMNET++    |
      nsb_client   [1]  <----> | nsb_server.py |         | nsb_payload.cc |
          ...                  |               |  <----> |  sendPayload() |
      nsb_client   [n]  <----> |_______________|         |________________|
*/

#ifndef __NSB_PAYLOAD_H
#define __NSB_PAYLOAD_H

#pragma once

#include <stdint.h>
#include <stdio.h>
#include <cstddef>

// Connection information.
#define O_SERVER_ADDRESS    "127.0.0.1"
#define O_SERVER_PORT       65432

#pragma pack(1) // Pack at one byte alignment

/*
STRUCT DEFINITIONS.

These struct definitions are used throughout the NSB interactions
*/

/* Header struct. */
typedef struct {
    uint8_t     type;
    uint32_t    len;
    uint32_t    srcid;
    uint32_t    dstid;
    uint32_t    msgid;
} oh_header_t;

/* Message delivery struct. */
typedef struct {
    int8_t      returnCode;
} oh_deliver_msg_ack_t;

/* Struct object to receive and return header and data from NSB message. */
struct nsbHeaderAndData_t{
    oh_header_t header;
    void *data;
};

/* Struct object provided to simulator clients to tag the message ID to messages. */
typedef struct {
    uint32_t msg_id;
    uint32_t msg_len;
    uint32_t msg_srcid;
    uint32_t msg_dstid;
    void * data;
} sim_packet_payload_t;


#pragma pack()

/* ENUMERATIONS.

These provide values for message types and error codes.
*/
enum OH_MSG_TYPES {
    OH_DELIVER_MSG = 31,
    OH_DELIVER_MSG_ACK,
    OH_RECV_MSG,
    OH_RESP_MSG,
    OH_MSG_GETSTATE,
    OH_MSG_STATE,
    OH_MSG_DUMMY
};

extern const char *_OH_MSG_TYPES_STR[];

#define OH_MSG_TYPES_STR(_i) \
    _OH_MSG_TYPES_STR[_i-OH_DELIVER_MSG]

enum ERROR_CODES {
    SUCCESS = 0,
    MESSAGE_NOT_FOUND,
    MESSAGE_ALREADY_DELIVERED,
    MESSAGE_IN_WRONG_QUEUE,
    MESSAGE_TO_SELF,
    OH_ERR_SOCK_CREATE,
    OH_ERR_INVALID_ADDR,
    OH_ERR_CONN_FAIL
};

extern const char *_ERROR_CODES[];

#define ERROR_CODE_STR(_i) \
    _ERROR_CODES[_i]

/*
LOW LEVEL FUNCTIONS (LEGACY).

These functions are one-directional and are used to send requests and receive 
responses from the NSB server. They are good to use when you are looking for 
non-blocking implementations, but will require that you implement both parts of
each transaction.
*/
int send_oh_recv_msg(int sock, uint32_t nodeid);
void * recv_oh_resp_msg(int sock, oh_header_t *header);
int send_oh_deliver_msg(int sock, uint32_t len, uint32_t srcid,
    uint32_t dstid, uint32_t msgid, void *data);
oh_deliver_msg_ack_t * recv_oh_deliver_msg_ack(int sock);
int getConnection (void);

/*
HIGH-LEVEL NSB FUNCTIONS.

These functions provide high-level access to the NSB server, and will take care 
of contacting the server, sending the appropriate messages, and receiving the 
appropriate responses.
*/

class NSBConnector {
    public:
        NSBConnector();
        ~NSBConnector();
        nsbHeaderAndData_t receiveMessageFromNsbServer(std::string sourceAddress);
        int notifyDeliveryToNsbServer(int msgLen, uint32_t sourceAddress, uint32_t destinationAddress, int msgId, void *message);
    private:
        int sock;
};

#endif
