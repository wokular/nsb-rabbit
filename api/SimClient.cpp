#include "SimClient.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <functional>
#include <google/protobuf/util/json_util.h>
#include "nsb_payload.pb.h"

SimRabbitManager::SimRabbitManager(const std::string &sim_name, std::function<void(const std::string &)> callback)
    : sim_name(sim_name), callback(callback), stopping(false)
{
    connect();
}

SimRabbitManager::~SimRabbitManager()
{
    stop();
}

void SimRabbitManager::connect()
{
    try
    {
        AmqpClient::Channel::OpenOpts opts;
        opts.host = "localhost";
        opts.port = 5672;
        opts.auth = AmqpClient::Channel::OpenOpts::BasicAuth("guest", "guest");

        // Create separate channels for publishing and consuming.
        publish_channel = AmqpClient::Channel::Open(opts);
        consume_channel = AmqpClient::Channel::Open(opts);

        main_exchange = "main_router";
        txq_name = "global_txq";

        // Setup exchange, queue, and binding using the publishing channel.
        publish_channel->DeclareExchange(main_exchange, AmqpClient::Channel::EXCHANGE_TYPE_DIRECT);
        publish_channel->DeclareQueue(txq_name, false, false, false, false);
        publish_channel->BindQueue(txq_name, main_exchange, txq_name);

        // Start consumer thread with the consuming channel.
        consume_thread = std::thread(&SimRabbitManager::consume, this);
    }
    catch (const std::exception &e)
    {
        std::cerr << "RabbitMQ connection error: " << e.what() << std::endl;
    }
}

void SimRabbitManager::send(const std::string &message, const std::string &dest_queue)
{
    try
    {
        AmqpClient::BasicMessage::ptr_t msg = AmqpClient::BasicMessage::Create(message);
        // Use the dedicated publishing channel.
        publish_channel->BasicPublish(main_exchange, dest_queue, msg);
    }
    catch (const std::exception &e)
    {
        std::cerr << "Failed to publish message: " << e.what() << std::endl;
    }
}

void SimRabbitManager::consume()
{
    try
    {
        // Store the consumer tag so we can cancel later.
        consumer_tag = consume_channel->BasicConsume(txq_name, "");
        while (!stopping)
        {
            AmqpClient::Envelope::ptr_t envelope = consume_channel->BasicConsumeMessage(consumer_tag);
            callback(envelope->Message()->Body());
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error consuming messages: " << e.what() << std::endl;
    }
}

void SimRabbitManager::stop()
{
    stopping = true;
    if (!consumer_tag.empty())
    {
        try
        {
            // Cancel the consumer to unblock the BasicConsumeMessage call.
            consume_channel->BasicCancel(consumer_tag);
        }
        catch (const std::exception &e)
        {
            std::cerr << "Error canceling consumer: " << e.what() << std::endl;
        }
    }
    if (consume_thread.joinable())
    {
        consume_thread.join();
    }
}

SimClient::SimClient(const std::string &sim_name) : sim_name(sim_name)
{
    rabbit_manager = std::make_unique<SimRabbitManager>(sim_name, [this](const std::string &msg)
                                                        { this->handleMessage(msg); });
}

void SimClient::send(const std::string &src_id, const std::string &dest_id, const std::string &message)
{
    nsb::Header header;
    header.set_datalen(message.size());
    header.set_srcid(src_id);
    header.set_dstid(dest_id);

    nsb::Message msg;
    msg.mutable_header()->CopyFrom(header);
    msg.set_body(message);

    std::string serialized_msg;
    msg.SerializeToString(&serialized_msg);

    rabbit_manager->send(serialized_msg, dest_id + "_rxq");
}

void SimClient::handleMessage(const std::string &message)
{
    nsb::Message msg;
    if (!msg.ParseFromString(message))
    {
        std::cerr << "Failed to parse incoming message" << std::endl;
        return;
    }

    const nsb::Header &header = msg.header();
    std::cout << "Received message from " << header.srcid() << " to " << header.dstid() << std::endl;

    // Echo message for now
    send(header.srcid(), header.dstid(), msg.body());
}
