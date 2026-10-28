#include <thread>
#include <chrono>
#include "protocol.h"
#include "sstream"
#include "iomanip"
#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

namespace { constexpr uint16_t kPort = 9000; }

int main()
{
    const int client_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (client_fd < 0) {
        std::cerr << "socket failed: " << std::strerror(errno) << '\n';
        return 1;
    }

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(kPort);
    server_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    if (connect(client_fd, reinterpret_cast<sockaddr*>(&server_addr),
                sizeof(server_addr)) < 0) {
        std::cerr << "connect failed: " << std::strerror(errno) << '\n';
        close(client_fd);
        return 1;
    }

    std::cout << "Connected to 127.0.0.1:" << kPort
              << "\nType a message and press Enter; type quit to exit.\n";
    std::string message;
    while (std::getline(std::cin, message)) {
        if (message == "quit") break;
        if (!tcp::send_message(client_fd, message)) {
    std::cerr << "[device] send failed\n";
    break;
}	
        std::string response;

if (!tcp::recv_message(client_fd, response)) {
    std::cerr << "[device] failed to receive response\n";
    break;
}

std::cout << "[device] response: "
          << response << '\n';

if (response != "GATEWAY_STORED") {
    std::cerr << "[device] message was not accepted: "
              << response << '\n';
    break;
}

std::this_thread::sleep_for(
    std::chrono::milliseconds(100)
);}
    close(client_fd);
    std::cout << "Client exited.\n";
    return 0;
}
