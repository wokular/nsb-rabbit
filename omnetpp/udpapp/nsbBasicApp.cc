//
// Copyright (C) 2000 Institut fuer Telematik, Universitaet Karlsruhe
// Copyright (C) 2004,2011 OpenSim Ltd.
//
// SPDX-License-Identifier: LGPL-3.0-or-later
//


//#include "inet/applications/udpapp/nsbBasicApp.h"

#include <sstream>
#include<arpa/inet.h>
#include<fstream>

#include "nsbBasicApp.h"

#include "inet/applications/base/ApplicationPacket_m.h"
#include "inet/common/ModuleAccess.h"
#include "inet/common/TagBase_m.h"
#include "inet/common/TimeTag_m.h"
#include "inet/common/lifecycle/ModuleOperations.h"
#include "inet/common/packet/Packet.h"
#include "inet/networklayer/common/FragmentationTag_m.h"
#include "inet/networklayer/common/L3AddressResolver.h"
#include "inet/transportlayer/contract/udp/UdpControlInfo_m.h"

#include "inet/applications/udpapp/nsb_api/nsb_payload.h"

namespace inet {

Define_Module(nsbBasicApp);

nsbBasicApp::~nsbBasicApp()
{
    cancelAndDelete(selfMsg);
}

void nsbBasicApp::initialize(int stage)
{
    ClockUserModuleMixin::initialize(stage);

    if (stage == INITSTAGE_LOCAL) {
        numSent = 0;
        //numReceived = 0;
        bytesSent = 0; //new
        WATCH(numSent);
        //WATCH(numReceived);

        localPort = par("localPort");
        destPort = par("destPort");
        startTime = par("startTime");
        stopTime = par("stopTime");
        packetName = par("packetName");
        dontFragment = par("dontFragment");
        if (stopTime >= CLOCKTIME_ZERO && stopTime < startTime)
            throw cRuntimeError("Invalid startTime/stopTime parameters");
        selfMsg = new ClockEvent("sendTimer");
//        connector = NSBConnector();
//        printf("Connector has been created.\n");
    }
}

void nsbBasicApp::finish()
{
    recordScalar("packets sent", numSent);
    recordScalar("packets received", numReceived);
    std::cout << getFullPath() << ": sent " << numSent << " packets"<<std::endl; //new addition
    std::cout << getFullPath() << ": sent " << bytesSent << " bytes"<<std::endl; //new addition
    //std::cout<<"Total sum of bytes sent in messages: "<< bytesSent << std::endl; //new addition
    //add write to csv here

    std::string filename = "SentResults.csv";
    std::fstream file;
    //checking if file exists, if it doesnt add header
    //file.open(filename);
    if(!file){
        std::cout<<"Creating and writing header to csv file:"<<std::endl;
        file.open(filename, std::ios::out);
        file<<"Host Name"<< ","<< "Packets Sent" << ","<<"Bytes Sent" << std::endl;
        file.close();

    }
    if(file){
        std::cout<<"File exists so just append to it"<<std::endl;
        file.open(filename, std::ios::out | std::ios::app);
        file<<getFullPath()<<","<<numSent <<"," <<bytesSent <<","<<std::endl;
        file.close();
    }



    ApplicationBase::finish();
}

void nsbBasicApp::setSocketOptions()
{
    int timeToLive = par("timeToLive");
    if (timeToLive != -1)
        socket.setTimeToLive(timeToLive);

    int dscp = par("dscp");
    if (dscp != -1)
        socket.setDscp(dscp);

    int tos = par("tos");
    if (tos != -1)
        socket.setTos(tos);

    const char *multicastInterface = par("multicastInterface");
    if (multicastInterface[0]) {
        IInterfaceTable *ift = getModuleFromPar<IInterfaceTable>(par("interfaceTableModule"), this);
        NetworkInterface *ie = ift->findInterfaceByName(multicastInterface);
        if (!ie)
            throw cRuntimeError("Wrong multicastInterface setting: no interface named \"%s\"", multicastInterface);
        socket.setMulticastOutputInterface(ie->getInterfaceId());
    }

    bool receiveBroadcast = par("receiveBroadcast");
    if (receiveBroadcast)
        socket.setBroadcast(true);

    bool joinLocalMulticastGroups = par("joinLocalMulticastGroups");
    if (joinLocalMulticastGroups) {
        MulticastGroupList mgl = getModuleFromPar<IInterfaceTable>(par("interfaceTableModule"), this)->collectMulticastGroups();
        socket.joinLocalMulticastGroups(mgl);
    }
    socket.setCallback(this);
}


L3Address nsbBasicApp::chooseDestAddr()
{
    int k = intrand(destAddresses.size());
    if (destAddresses[k].isUnspecified() || destAddresses[k].isLinkLocal()) {
        L3AddressResolver().tryResolve(destAddressStr[k].c_str(), destAddresses[k]);
    }
    return destAddresses[k];
}


void nsbBasicApp::sendPacket()
{
    std::ostringstream str;

    //getting the Ipv4 addr of parent module, which is the srcAddr
    L3Address srcAddr_l3 = L3AddressResolver().addressOf(getParentModule());
    Ipv4Address srcAddr_ipv4 = srcAddr_l3.toIpv4();
    std::string srcAddr = srcAddr_ipv4.str();

    //Receiving message from NSB server
    nsbHeaderAndData_t nsbHeaderAndData;
    nsbHeaderAndData = connector.receiveMessageFromNsbServer(srcAddr);
    oh_header_t oh_header;
    oh_header = nsbHeaderAndData.header ;

    //checking that there is some message received
    if(oh_header.len > 0){

        // Create simulation payload and setting it with values
        sim_packet_payload_t sim_payload;
        sim_payload.msg_id = nsbHeaderAndData.header.msgid;
        sim_payload.data = nsbHeaderAndData.data;
        sim_payload.msg_len = nsbHeaderAndData.header.len;
        sim_payload.msg_srcid = nsbHeaderAndData.header.srcid;
        sim_payload.msg_dstid = nsbHeaderAndData.header.dstid;

        //Creating a new packet, and setting rawBytesData to the message's data
        auto rawBytesData = makeShared<BytesChunk>();
        std::vector<uint8_t> vec;
        // Copy struct bytes of sim_payload to vector vec.
        uint8_t *msgTemp_c = reinterpret_cast<uint8_t *> (&sim_payload);
        vec = std::vector<uint8_t>(msgTemp_c, msgTemp_c + sizeof sim_payload);
        rawBytesData->setBytes(vec);
        Packet *packet = new Packet(str.str().c_str(), rawBytesData);

        //host byte order to network byte order and conversions for address resolution
        uint32_t destAddr_int =  htonl(sim_payload.msg_dstid);
        std::string destAddr_str =  std::string(inet_ntoa(*(struct in_addr *)&destAddr_int)); //converting to string
        const char * destAddr_c = destAddr_str.c_str();  //converting to const char* to pass as argument to resolve to L3Address

        emit(packetSentSignal, packet);
        socket.sendTo(packet, L3AddressResolver().resolve(destAddr_c), destPort);

        numSent++;
        bytesSent+=sim_payload.msg_len;


        if (dontFragment)
            packet->addTag<FragmentationReq>()->setDontFragment(true);

    }

    //no message received from the NSB server
    else{
//        std::cout<<"No message to be sent!!!!!"<<std::endl;
    }

}

void nsbBasicApp::processStart()
{
    socket.setOutputGate(gate("socketOut"));
    const char *localAddress = par("localAddress");
    socket.bind(*localAddress ? L3AddressResolver().resolve(localAddress) : L3Address(), localPort);
    setSocketOptions();

    const char *destAddrs = par("destAddresses");
    cStringTokenizer tokenizer(destAddrs);
    const char *token;

    while ((token = tokenizer.nextToken()) != nullptr) {
        destAddressStr.push_back(token);
        L3Address result;
        L3AddressResolver().tryResolve(token, result);
        if (result.isUnspecified())
            EV_ERROR << "cannot resolve destination address: " << token << endl;
        destAddresses.push_back(result);
    }

    if (!destAddresses.empty()) {
        selfMsg->setKind(SEND);
        processSend();
    }
    else {
        if (stopTime >= CLOCKTIME_ZERO) {
            selfMsg->setKind(STOP);
            scheduleClockEventAt(stopTime, selfMsg);
        }
    }
}

void nsbBasicApp::processSend()
{
    sendPacket();
    clocktime_t d = par("sendInterval");
    if (stopTime < CLOCKTIME_ZERO || getClockTime() + d < stopTime) {
        selfMsg->setKind(SEND);
        scheduleClockEventAfter(d, selfMsg);
    }


    else {
        selfMsg->setKind(STOP);
        scheduleClockEventAt(stopTime, selfMsg);
    }

}

void nsbBasicApp::processStop()
{
    socket.close();
}

void nsbBasicApp::handleMessageWhenUp(cMessage *msg)
{
    if (msg->isSelfMessage()) {
        ASSERT(msg == selfMsg);
        switch (selfMsg->getKind()) {
            case START:
                processStart();
                break;

            case SEND:
                processSend();
                break;

            case STOP:
                processStop();
                break;

            default:
                throw cRuntimeError("Invalid kind %d in self message", (int)selfMsg->getKind());
        }
    }
    else
        socket.processMessage(msg);
}

void nsbBasicApp::socketDataArrived(UdpSocket *socket, Packet *packet)
{
    // process incoming packet
    processPacket(packet);
}

void nsbBasicApp::socketErrorArrived(UdpSocket *socket, Indication *indication)
{
    EV_WARN << "Ignoring UDP error report " << indication->getName() << endl;
    delete indication;
}

void nsbBasicApp::socketClosed(UdpSocket *socket)
{
    if (operationalState == State::STOPPING_OPERATION)
        startActiveOperationExtraTimeOrFinish(par("stopOperationExtraTime"));
}

void nsbBasicApp::refreshDisplay() const
{
    ApplicationBase::refreshDisplay();

    char buf[100];
    sprintf(buf, "rcvd: %d pks\nsent: %d pks", numReceived, numSent);
    getDisplayString().setTagArg("t", 0, buf);
}

void nsbBasicApp::processPacket(Packet *pk)
{
    emit(packetReceivedSignal, pk);
    EV_INFO << "Received packet: " << UdpSocket::getReceivedPacketInfo(pk) << endl;
    delete pk;
    numReceived++;
}

void nsbBasicApp::handleStartOperation(LifecycleOperation *operation)
{
    clocktime_t start = std::max(startTime, getClockTime());
    if ((stopTime < CLOCKTIME_ZERO) || (start < stopTime) || (start == stopTime && startTime == stopTime)) {
        selfMsg->setKind(START);
        scheduleClockEventAt(start, selfMsg);
    }
}

void nsbBasicApp::handleStopOperation(LifecycleOperation *operation)
{
    cancelEvent(selfMsg);
    socket.close();
    delayActiveOperationFinish(par("stopOperationTimeout"));
}

void nsbBasicApp::handleCrashOperation(LifecycleOperation *operation)
{
    cancelClockEvent(selfMsg);
    socket.destroy(); // TODO  in real operating systems, program crash detected by OS and OS closes sockets of crashed programs.
}

} // namespace inet

