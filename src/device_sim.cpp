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
  double data=25.6;
int i=1;
while(i<20){
 data+=0.1;
std::ostringstream oss;

oss<<"device|"<< i <<"|"<< "00000000000000000000000000000001" <<"|"<<data;
        std::string message_sim=oss.str();
tcp::send_message(client_fd,message_sim);
usleep(100000);
i++;
}
    while (std::getline(std::cin, message)) {
        if (message == "quit") break;
        /*if (!tcp::send_message(client_fd, message)) {
            std::cerr << "send_message failed: " << std::strerror(errno) << '\n';
            break;
        }*/
	
        std::string reply;
        if (!tcp::recv_message(client_fd, reply)) {
            std::cerr << "Server closed the connection or receive failed.\n";
            break;
        }
        std::cout << "Reply: " << reply << '\n';
	usleep(100);
    }

    close(client_fd);
    std::cout << "Client exited.\n";
    return 0;
}
