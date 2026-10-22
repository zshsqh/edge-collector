#include "protocol.h"
#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string>

namespace {
constexpr uint16_t kPort = 9000;

void handle_client(int client_fd)
{
    while (true) {
        std::string message;
        if (!tcp::recv_message(client_fd, message)) {
            std::cout << "Client closed or receive failed.\n";
            break;
        }
        std::cout << "Received (" << message.size() << " bytes): "
                  << message << '\n';
        if (!tcp::send_message(client_fd, "ACK: " + message)) {
            std::cerr << "send_message failed: " << std::strerror(errno) << '\n';
            break;
        }
    }
    close(client_fd);
}
}

int main()
{
    const int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "socket failed: " << std::strerror(errno) << '\n';
        return 1;
    }

    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        std::cerr << "setsockopt failed: " << std::strerror(errno) << '\n';
        close(server_fd);
        return 1;
    }

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(kPort);
    server_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    if (bind(server_fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) < 0) {
        std::cerr << "bind failed: " << std::strerror(errno) << '\n';
        close(server_fd);
        return 1;
    }
    if (listen(server_fd, 16) < 0) {
        std::cerr << "listen failed: " << std::strerror(errno) << '\n';
        close(server_fd);
        return 1;
    }

    std::cout << "Day10 server listening on 127.0.0.1:" << kPort << '\n';
    while (true) {
        sockaddr_in peer_addr{};
        socklen_t peer_len = sizeof(peer_addr);
        const int client_fd = accept(server_fd,
            reinterpret_cast<sockaddr*>(&peer_addr), &peer_len);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            std::cerr << "accept failed: " << std::strerror(errno) << '\n';
            continue;
        }

        char peer_ip[INET_ADDRSTRLEN] = {};
        inet_ntop(AF_INET, &peer_addr.sin_addr, peer_ip, sizeof(peer_ip));
        std::cout << "Client connected: " << peer_ip << ':'
                  << ntohs(peer_addr.sin_port) << '\n';

        // Intentionally single-threaded: one client is handled at a time.
        handle_client(client_fd);
    }
    close(server_fd);
    return 0;
}
