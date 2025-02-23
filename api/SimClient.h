#include <SimpleAmqpClient/SimpleAmqpClient.h>
#include <iostream>
#include <string>
#include <memory>
#include <thread>
#include <chrono>
#include <functional>
#include <google/protobuf/message.h>
#include "nsb_payload.pb.h"

class SimRabbitManager
{
public:
    SimRabbitManager(const std::string &sim_name, std::function<void(const std::string &)> callback);
    ~SimRabbitManager();

    void connect();
    void stop();
    void send(const std::string &message, const std::string &dest_queue);

private:
    std::string sim_name;
    std::string main_exchange;
    std::string txq_name;
    AmqpClient::Channel::ptr_t publish_channel;
    AmqpClient::Channel::ptr_t consume_channel;
    std::function<void(const std::string &)> callback;
    bool stopping;
    std::thread consume_thread;
    std::string consumer_tag;

    void consume();
};

class SimClient
{
public:
    SimClient(const std::string &sim_name);
    void send(const std::string &src_id, const std::string &dest_id, const std::string &message);

private:
    std::string sim_name;
    std::unique_ptr<SimRabbitManager> rabbit_manager;
    void handleMessage(const std::string &message);
};
