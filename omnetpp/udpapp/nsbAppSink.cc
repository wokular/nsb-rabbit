//
// Copyright (C) 2000 Institut fuer Telematik, Universitaet Karlsruhe
//
// SPDX-License-Identifier: LGPL-3.0-or-later
//

#include "nsbAppSink.h"

#include "inet/applications/base/ApplicationPacket_m.h"
#include "inet/common/ModuleAccess.h"
#include "inet/common/packet/Packet.h"
#include "inet/networklayer/common/L3AddressResolver.h"
#include "inet/transportlayer/contract/udp/UdpControlInfo_m.h"

#include "inet/applications/udpapp/nsb_api/nsb_payload.h"
#include<fstream>
namespace inet {

Define_Module(nsbAppSink);

nsbAppSink::~nsbAppSink()
{
    cancelAndDelete(selfMsg);
}

void nsbAppSink::initialize(int stage)
{
    ApplicationBase::initialize(stage);

    if (stage == INITSTAGE_LOCAL) {
        numReceived = 0;
        bytesReceived =0; //new
        WATCH(numReceived);

        localPort = par("localPort");
        startTime = par("startTime");
        stopTime = par("stopTime");
        if (stopTime >= SIMTIME_ZERO && stopTime < startTime)
            throw cRuntimeError("Invalid startTime/stopTime parameters");
        selfMsg = new cMessage("UDPSinkTimer");
//        connector = NSBConnector();
//        printf("Connector has been created.\n");
    }
}

void nsbAppSink::handleMessageWhenUp(cMessage *msg)
{
    if (msg->isSelfMessage()) {
        ASSERT(msg == selfMsg);
        switch (selfMsg->getKind()) {
            case START:
                processStart();
                break;

            case STOP:
                processStop();
                break;

            default:
                throw cRuntimeError("Invalid kind %d in self message", (int)selfMsg->getKind());
        }
    }
    else if (msg->arrivedOn("socketIn"))
        socket.processMessage(msg);
    else
        throw cRuntimeError("Unknown incoming gate: '%s'", msg->getArrivalGate()->getFullName());
}

void nsbAppSink::socketDataArrived(UdpSocket *socket, Packet *packet)
{
    // process incoming packet
    processPacket(packet);
}

void nsbAppSink::socketErrorArrived(UdpSocket *socket, Indication *indication)
{
    EV_WARN << "Ignoring UDP error report " << indication->getName() << endl;
    delete indication;
}

void nsbAppSink::socketClosed(UdpSocket *socket)
{
    if (operationalState == State::STOPPING_OPERATION)
        startActiveOperationExtraTimeOrFinish(par("stopOperationExtraTime"));
}

void nsbAppSink::refreshDisplay() const
{
    ApplicationBase::refreshDisplay();

    char buf[50];
    sprintf(buf, "rcvd: %d pks", numReceived);
    getDisplayString().setTagArg("t", 0, buf);
}

void nsbAppSink::finish()
{
    ApplicationBase::finish();
    EV_INFO << getFullPath() << ": received " << numReceived << " packets\n";
    std::cout << getFullPath() << ": received " << numReceived << " packets"<<std::endl; //new addition
    std::cout << getFullPath() << ": received " << bytesReceived << " bytes"<<std::endl; //new addition
    //std::cout<<"Total sum of bytes received in messages: "<< bytesReceived << std::endl; //new addition

    //add write to csv here

    std::string filename = "RcvdResults.csv";
    std::fstream file;
    //checking if file exists, if it doesnt add header
    if(!file){
        std::cout<<"Creating and writing header to csv file:"<<std::endl;
        file.open(filename, std::ios::out);
        file<<"Host Name"<< ","<< "Packets Received" << ","<<"Bytes Received" << std::endl;
        file.close();

    }
    if(file){
        std::cout<<"File exists so just append to it"<<std::endl;
        file.open(filename, std::ios::out | std::ios::app);
        file<<getFullPath()<<","<<numReceived <<"," <<bytesReceived <<","<<std::endl;
        file.close();
    }

}

void nsbAppSink::setSocketOptions()
{
    bool receiveBroadcast = par("receiveBroadcast");
    if (receiveBroadcast)
        socket.setBroadcast(true);

    MulticastGroupList mgl = getModuleFromPar<IInterfaceTable>(par("interfaceTableModule"), this)->collectMulticastGroups();
    socket.joinLocalMulticastGroups(mgl);

    // join multicastGroup
    const char *groupAddr = par("multicastGroup");
    multicastGroup = L3AddressResolver().resolve(groupAddr);
    if (!multicastGroup.isUnspecified()) {
        if (!multicastGroup.isMulticast())
            throw cRuntimeError("Wrong multicastGroup setting: not a multicast address: %s", groupAddr);
        socket.joinMulticastGroup(multicastGroup);
    }
    socket.setCallback(this);
}

void nsbAppSink::processStart()
{
    socket.setOutputGate(gate("socketOut"));
    socket.bind(localPort);
    setSocketOptions();

    if (stopTime >= SIMTIME_ZERO) {
        selfMsg->setKind(STOP);
        scheduleAt(stopTime, selfMsg);
    }
}

void nsbAppSink::processStop()
{
    if (!multicastGroup.isUnspecified())
        socket.leaveMulticastGroup(multicastGroup); // FIXME should be done by socket.close()
    socket.close();
}

void nsbAppSink::processPacket(Packet *pk)
{

    //extracting payload from received packet
    const auto& payload = pk->peekData<BytesChunk>();
    std::vector<uint8_t> vec = payload->getBytes();
    // Convert vector of bytes back to sim_packet_payload_t struct.
    sim_packet_payload_t* sim_payload = reinterpret_cast<sim_packet_payload_t*>(&vec[0]);

    //extracting info from sim_payload
    uint32_t msgid = sim_payload->msg_id;
    uint32_t msglen = sim_payload->msg_len;
    void* msgData = malloc(msglen);
    memcpy(msgData, sim_payload->data, msglen);
    uint32_t msg_srcid = sim_payload->msg_srcid;
    uint32_t msg_dstid = sim_payload->msg_dstid;


    //Notifying about delivery to NSB server
    int result = connector.notifyDeliveryToNsbServer(msglen, msg_srcid, msg_dstid, msgid, msgData);
    free(msgData); // Free msgData.

    EV_INFO << "Received packet: " << UdpSocket::getReceivedPacketInfo(pk) << endl;
    emit(packetReceivedSignal, pk);
    delete pk;

    numReceived++;
    bytesReceived+=msglen;
}

void nsbAppSink::handleStartOperation(LifecycleOperation *operation)
{
    simtime_t start = std::max(startTime, simTime());
    if ((stopTime < SIMTIME_ZERO) || (start < stopTime) || (start == stopTime && startTime == stopTime)) {
        selfMsg->setKind(START);
        scheduleAt(start, selfMsg);
    }
}

void nsbAppSink::handleStopOperation(LifecycleOperation *operation)
{
    cancelEvent(selfMsg);
    if (!multicastGroup.isUnspecified())
        socket.leaveMulticastGroup(multicastGroup); // FIXME should be done by socket.close()
    socket.close();
    delayActiveOperationFinish(par("stopOperationTimeout"));
}

void nsbAppSink::handleCrashOperation(LifecycleOperation *operation)
{
    cancelEvent(selfMsg);
    if (operation->getRootModule() != getContainingNode(this)) { // closes socket when the application crashed only
        if (!multicastGroup.isUnspecified())
            socket.leaveMulticastGroup(multicastGroup); // FIXME should be done by socket.close()
        socket.destroy(); // TODO  in real operating systems, program crash detected by OS and OS closes sockets of crashed programs.
    }
}

} // namespace inet

