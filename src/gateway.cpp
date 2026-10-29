#include "local_store.h"
#include <poll.h>
#include <pthread.h>
#include <sys/signalfd.h>
#include <sys/time.h>
#include "protocol.h"
#include "device_message.h"
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>
#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include "net_timeout.h"
#include <signal.h>
namespace {
constexpr uint16_t kGatewayPort = 9000;
constexpr uint16_t kCenterPort = 9001;

// 将一条已经校验过的消息转发给中心服务。

bool forward_to_center(
    const std::string& message_id,
    const std::string& raw,
    std::string& center_response)
{
    const int fd = socket(AF_INET, SOCK_STREAM, 0);

if (fd < 0)
    return false;

sockaddr_in addr{};
addr.sin_family = AF_INET;
addr.sin_port = htons(kCenterPort);
addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

bool ok =
    set_io_timeout(fd, 2000) &&
    connect_with_timeout(
        fd,
        reinterpret_cast<sockaddr*>(&addr),
        sizeof(addr),
        2000
    );

if (ok) {
    const std::string envelope =
        message_id + "\n" + raw;

    ok =
        tcp::send_message(fd, envelope) &&
        tcp::recv_message(fd, center_response);
}

if (!ok) {
    const int saved_errno = errno;

    std::cerr << "[network] center communication failed: "
              << std::strerror(saved_errno) << '\n';

    close(fd);
    errno = saved_errno;
    return false;
}

close(fd);
return true;
}


void retry_pending(
    LocalStore& store,
    std::atomic<bool>& running)
{
    sqlite3_int64 cursor = 0;

    while (running.load()) {
        std::vector<PendingMessage> messages;
        std::string error;

        if (!store.load_pending_after(
                cursor, messages, error)) {
            std::cerr << "[retry] SQLite error: "
                      << error << '\n';

            cursor = 0;
            std::this_thread::sleep_for(
                std::chrono::seconds(2));
            continue;
        }

        // 没有更多待传记录，重新从头扫描
        if (messages.empty()) {
            cursor = 0;
            std::this_thread::sleep_for(
                std::chrono::seconds(2));
            continue;
        }

        bool failed = false;

        for (const auto& item : messages) {
            if (!running.load())
                break;

            cursor = item.id;

            // 避免历史非法记录进入重试链路
            DeviceMessage parsed;
            std::string parse_error;

            if (!parse_device_message(
        item.payload, parsed, parse_error)) {

    std::string db_error;

    if (!store.mark_quarantined(
            item.id,
            "invalid payload: " + parse_error,
            db_error)) {

        std::cerr << "[retry] quarantine failed: "
                  << db_error << '\n';

        failed = true;
        break;
    }

    std::cerr << "[retry] quarantined id="
              << item.id << '\n';

    continue;
}

            // 必须使用原来的本地记录ID
            const std::string message_id =
                "gateway01:" + std::to_string(item.id);

            std::string response;

            if (!forward_to_center(
                    message_id,
                    item.payload,
                    response)) {
                std::cerr << "[retry] center unavailable\n";
                failed = true;
                break;
            }

            if (!parse_device_message(
        item.payload, parsed, parse_error)) {

    std::string db_error;

    if (!store.mark_quarantined(
            item.id,
            "invalid payload: " + parse_error,
            db_error)) {

        std::cerr << "[retry] quarantine failed: "
                  << db_error << '\n';

        failed = true;
        break;
    }

    std::cerr << "[retry] quarantined id="
              << item.id << '\n';

    continue;
}

            if (!store.mark_delivered(item.id, error)) {
                std::cerr << "[retry] update failed: "
                          << error << '\n';
                failed = true;
                break;
            }

            std::cout << "[retry] delivered id="
                      << item.id << '\n';
        }

        if (failed || messages.size() < 100) {
            cursor = 0;

            std::this_thread::sleep_for(
                std::chrono::seconds(2));
        }
    }
}

void handle_device(int client_fd, LocalStore& store,int signal_fd)
{

    while (true) {
        std::string raw;
pollfd stop_event{};
 stop_event.fd = signal_fd;
 stop_event.events = POLLIN;

 if (poll(&stop_event, 1, 0) > 0 &&
     (stop_event.revents & POLLIN)) {
     break;
 }

        if (!tcp::recv_message(client_fd, raw)) {
            std::cout << "[gateway] device disconnected\n";
            break;
        }

        DeviceMessage msg;
        std::string error;

        // 第一步：先验证消息
        if (!parse_device_message(raw, msg, error)) {
            if (!tcp::send_message(
                    client_fd, "REJECTED: " + error)) {
                break;
            }

            continue;
        }
if (msg.event_id.empty()) {
    if (!tcp::send_message(
            client_fd, "EVENT_ID_REQUIRED")) {
        break;
    }
    continue;
}
        // 第二步：保存并取得本地记录ID
        sqlite3_int64 local_id = 0;

        if (!store.append(msg, raw, local_id, error)) {
            std::cerr << "[gateway] SQLite error: "
                      << error << '\n';

            if (!tcp::send_message(
                    client_fd, "STORE_FAILED")) {
                break;
            }

            continue;
        }

        std::cout << "[gateway] locally stored id="
                  << local_id
                  << " device=" << msg.device_id
                  << " seq=" << msg.sequence << '\n';

        // 第三步：只向设备发送一次本地保存确认
        if (!tcp::send_message(
                client_fd, "GATEWAY_STORED")) {
            // 数据已经提交；即使设备没收到确认，记录也保留
            break;
        }

        
}
    close(client_fd);
}}
int main()
{
LocalStore store;
std::string error;

if (!store.open("data/gateway.db", error)) {
    std::cerr << "[gateway] SQLite init failed: "
              << error << '\n';
    return 1;
}
    const int server_fd = socket(AF_INET, SOCK_STREAM, 0);

    if (server_fd < 0) {
        std::cerr << "socket failed: "
                  << std::strerror(errno) << '\n';
        return 1;
    }

    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR,
               &reuse, sizeof(reuse));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(kGatewayPort);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    if (bind(server_fd, reinterpret_cast<sockaddr*>(&addr),
             sizeof(addr)) < 0) {
        std::cerr << "bind failed: "
                  << std::strerror(errno) << '\n';
        close(server_fd);
        return 1;
    }

    if (listen(server_fd, 16) < 0) {
        std::cerr << "listen failed: "
                  << std::strerror(errno) << '\n';
        close(server_fd);
        return 1;
    }

sigset_t signal_mask;
sigemptyset(&signal_mask);

sigaddset(&signal_mask, SIGINT);
sigaddset(&signal_mask, SIGTERM);

// 必须在创建后台线程之前屏蔽这些信号
int mask_rc = pthread_sigmask(
    SIG_BLOCK, &signal_mask, nullptr
);

if (mask_rc != 0) {
    std::cerr << "pthread_sigmask failed: "
              << std::strerror(mask_rc) << '\n';
    close(server_fd);
    return 1;
}

int signal_fd = signalfd(
    -1,
    &signal_mask,
    SFD_CLOEXEC | SFD_NONBLOCK
);

if (signal_fd < 0) {
    std::cerr << "signalfd failed: "
              << std::strerror(errno) << '\n';
    close(server_fd);
    return 1;
}

std::atomic<bool> running{true};

std::thread retry_thread([&]() {
    retry_pending(store, running);
});
    std::cout << "[gateway] listening on 127.0.0.1:"
              << kGatewayPort << '\n';        
while (true) {
    pollfd fds[2]{};

    fds[0].fd = server_fd;
    fds[0].events = POLLIN;

    fds[1].fd = signal_fd;
    fds[1].events = POLLIN;

    const int n = poll(fds, 2, 500);

    if (n < 0) {
        if (errno == EINTR)
            continue;

        std::cerr << "[gateway] poll failed: "
                  << std::strerror(errno) << '\n';
        break;
    }

    if (n == 0)
        continue;

    // 优先处理关闭信号
    if (fds[1].revents & POLLIN) {
        std::cout << "[gateway] shutdown requested\n";
        break;
    }

    if (fds[0].revents &
        (POLLERR | POLLHUP | POLLNVAL)) {
        std::cerr << "[gateway] listener error\n";
        break;
    }

    if (fds[0].revents & POLLIN) {
        const int client_fd =
            accept(server_fd, nullptr, nullptr);

        if (client_fd < 0) {
            if (errno == EINTR)
                continue;

            std::cerr << "[gateway] accept failed: "
                      << std::strerror(errno) << '\n';
            continue;
        }

        // 对设备连接也设置I/O超时
        if (!set_io_timeout(client_fd, 2000)) {
            close(client_fd);
            continue;
        }

        handle_device(client_fd, store, signal_fd);
    }
}
running.store(false);

if (retry_thread.joinable()) {
    retry_thread.join();
}

close(signal_fd);
close(server_fd);

std::cout << "[gateway] shutdown complete\n";
return 0;
}
