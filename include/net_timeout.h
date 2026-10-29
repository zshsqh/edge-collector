
#pragma once

#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

// 设置发送和接收超时
inline bool set_io_timeout(int fd, int timeout_ms)
{
    timeval tv{};
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;

    return setsockopt(
               fd, SOL_SOCKET, SO_SNDTIMEO,
               &tv, sizeof(tv)) == 0
        && setsockopt(
               fd, SOL_SOCKET, SO_RCVTIMEO,
               &tv, sizeof(tv)) == 0;
}

// 限制 TCP 连接建立过程的等待时间
inline bool connect_with_timeout(
    int fd,
    const sockaddr* addr,
    socklen_t addr_len,
    int timeout_ms)
{
    if (timeout_ms <= 0) {
        errno = EINVAL;
        return false;
    }

    // 1. 保存原来的文件状态标志
    int old_flags = fcntl(fd, F_GETFL, 0);
    if (old_flags < 0)
        return false;

    // 2. 临时切换到非阻塞模式
    if (fcntl(fd, F_SETFL, old_flags | O_NONBLOCK) < 0)
        return false;

    int error = 0;
    int rc = connect(fd, addr, addr_len);

    if (rc < 0) {
        if (errno != EINPROGRESS && errno != EINTR) {
            error = errno;
        } else {
            // 3. 等待连接完成，使用总截止时间
            const auto deadline =
                std::chrono::steady_clock::now()
                + std::chrono::milliseconds(timeout_ms);

            while (true) {
                auto remaining =
                    std::chrono::duration_cast<
                        std::chrono::milliseconds>(
                        deadline -
                        std::chrono::steady_clock::now()
                    ).count();

                if (remaining <= 0) {
                    error = ETIMEDOUT;
                    break;
                }

                pollfd pfd{};
                pfd.fd = fd;
                pfd.events = POLLOUT;

                int n = poll(
                    &pfd, 1,
                    static_cast<int>(remaining)
                );

                if (n < 0 && errno == EINTR)
                    continue;

                if (n == 0) {
                    error = ETIMEDOUT;
                    break;
                }

                if (n < 0) {
                    error = errno;
                    break;
                }

                // 4. 可写不等于连接成功，必须检查SO_ERROR
                int socket_error = 0;
                socklen_t len = sizeof(socket_error);

                if (getsockopt(
                        fd, SOL_SOCKET, SO_ERROR,
                        &socket_error, &len) < 0) {
                    error = errno;
                } else {
                    error = socket_error;
                }

                break;
            }
        }
    }

    // 5. 恢复原来的阻塞状态
    if (fcntl(fd, F_SETFL, old_flags) < 0 &&
        error == 0) {
        error = errno;
    }

    if (error != 0) {
        errno = error;
        return false;
    }

    return true;
}

