#include "SimClient.h"

#include <iostream>
#include <csignal>
#include <thread>
#include <chrono>

// Global flag used for graceful shutdown.
volatile std::sig_atomic_t running = 1;

// Custom signal handler to catch Ctrl+C (SIGINT).
void signalHandler(int signum)
{
    static bool alreadyInterrupted = false;

    // Only handle the first SIGINT in a custom way.
    if (!alreadyInterrupted)
    {
        std::cout << "\nInterrupt signal (" << signum << ") received. Shutting down..." << std::endl;
        running = 0;
        alreadyInterrupted = true;

        // Reassign SIGINT to default behavior for subsequent interrupts.
        std::signal(SIGINT, SIG_DFL);
    }
}

int main()
{
    // Set up our signal handler for graceful shutdown.
    std::signal(SIGINT, signalHandler);

    // Create the SimClient. It will connect to RabbitMQ and
    // begin consuming messages from the global_txq.
    SimClient simClient("SimClient");
    std::cout << "SimClient connected to RabbitMQ and listening on global_txq." << std::endl;

    // Keep this thread alive until a SIGINT is received (Ctrl+C).
    while (running)
    {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Once running is set to 0, we exit the loop.
    // The SimClient destructor (and SimRabbitManager destructor) will
    // stop consuming and close connections gracefully.
    std::cout << "SimClient shut down." << std::endl;
    return 0;
}