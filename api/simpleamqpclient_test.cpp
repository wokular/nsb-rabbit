#include <SimpleAmqpClient/SimpleAmqpClient.h>
#include <iostream>

int main() {
  try {
    // 1. Set up OpenOpts for the RabbitMQ connection
    AmqpClient::Channel::OpenOpts opts;
    opts.host = "localhost"; // Set the hostname
    opts.auth = AmqpClient::Channel::OpenOpts::BasicAuth("guest", "guest");

    // 2. Use the new `Open` method
    AmqpClient::Channel::ptr_t channel = AmqpClient::Channel::Open(opts);

    std::string queue_name = "test_queue";

    // 3. Declare a queue
    channel->DeclareQueue(queue_name, false, true, false, false);

    // 4. Publish a message
    std::string message = "Hello, RabbitMQ!";
    AmqpClient::BasicMessage::ptr_t msg =
        AmqpClient::BasicMessage::Create(message);
    channel->BasicPublish("", queue_name, msg);
    std::cout << "Sent: " << message << std::endl;

    // 5. Consume a message
    std::string consumer_tag = channel->BasicConsume(queue_name, "");
    AmqpClient::Envelope::ptr_t envelope =
        channel->BasicConsumeMessage(consumer_tag);
    std::cout << "Received: " << envelope->Message()->Body() << std::endl;

  } catch (const std::exception &e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}
