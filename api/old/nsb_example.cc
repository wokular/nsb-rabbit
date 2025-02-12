#include ".../nsb_api/nsb_payload.h"

// HEADER FILE
class MyObject {
    ...
    protected:
        ...
        NSBConnector connector = NSBConnector();
        ...
}

// CPP FILE
...
void MyObject::sendPacket()
{
    ...
    // Receiving message from the NSB server.
    nsbHeaderAndData_t nsbHeaderAndData;
    nsbHeaderAndData = connector.receiveMessage(srcAddr);
    // Parse values from returned header and data.
    src_addr = nsbHeaderAndData.header.src_id;
    dest_addr = nsbHeaderAndData.header.dest_id;
    msg_id = nsbHeaderAndData.header.msg_id;
    msg_len = nsbHeaderAndData.header.msg_len;
    data = nsbHeaderAndData.data;
    ...
}
...
void MyObject::processPacket()
{
    ...
    // If packet has arrived at destination,
    // notify about delivery to NSB server.
    int result = connector.notifyDeliveryToNsbServer
        (msg_len, source, dest, msgid, msgData);
}