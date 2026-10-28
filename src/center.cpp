#include <sqlite3.h>
#include "protocol.h"
#include "device_message.h"
#include <cstdlib>
#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

namespace {
constexpr uint16_t kCenterPort = 9001;

bool exec_sql(
    sqlite3* db,
    const char* sql,
    std::string& error)
{
    char* detail = nullptr;

    const int rc = sqlite3_exec(
        db, sql, nullptr, nullptr, &detail
    );

    if (rc != SQLITE_OK) {
        error = detail ? detail : sqlite3_errmsg(db);
        sqlite3_free(detail);
        return false;
    }

    return true;
}

bool init_center_db(sqlite3*& db, std::string& error)
{
    const int rc = sqlite3_open("data/center.db", &db);

    if (rc != SQLITE_OK) {
        error = db ? sqlite3_errmsg(db) : "sqlite open failed";
        return false;
    }

    if (sqlite3_busy_timeout(db, 2000) != SQLITE_OK) {
        error = sqlite3_errmsg(db);
        return false;
    }

    if (!exec_sql(db, "PRAGMA journal_mode=WAL;", error))
        return false;

    if (!exec_sql(db, "PRAGMA synchronous=FULL;", error))
        return false;

    return exec_sql(
        db,
        "CREATE TABLE IF NOT EXISTS received_messages ("
        " message_id TEXT PRIMARY KEY,"
        " device_id TEXT NOT NULL,"
        " seq TEXT NOT NULL,"
        " timestamp_ms INTEGER NOT NULL,"
        " temperature_c REAL NOT NULL,"
        " payload TEXT NOT NULL,"
        " received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP"
        ");",
        error
    );
}

bool persist_center_message(
    sqlite3* db,
    const std::string& message_id,
    const DeviceMessage& msg,
    const std::string& raw,
    std::string& error)
{
    if (!exec_sql(db, "BEGIN IMMEDIATE;", error))
        return false;

    const char* insert_sql =
        "INSERT INTO received_messages("
        "message_id, device_id, seq, timestamp_ms,"
        "temperature_c, payload"
        ") VALUES(?1, ?2, ?3, ?4, ?5, ?6) "
        "ON CONFLICT(message_id) DO NOTHING;";

    sqlite3_stmt* stmt = nullptr;

    int rc = sqlite3_prepare_v2(
        db, insert_sql, -1, &stmt, nullptr
    );

    const std::string seq = std::to_string(msg.sequence);

    if (rc == SQLITE_OK)
        rc = sqlite3_bind_text(
            stmt, 1, message_id.c_str(), -1, SQLITE_TRANSIENT);

    if (rc == SQLITE_OK)
        rc = sqlite3_bind_text(
            stmt, 2, msg.device_id.c_str(), -1, SQLITE_TRANSIENT);

    if (rc == SQLITE_OK)
        rc = sqlite3_bind_text(
            stmt, 3, seq.c_str(), -1, SQLITE_TRANSIENT);

    if (rc == SQLITE_OK)
        rc = sqlite3_bind_int64(
            stmt, 4, msg.timestamp_ms);

    if (rc == SQLITE_OK)
        rc = sqlite3_bind_double(
            stmt, 5, msg.temperature_c);

    if (rc == SQLITE_OK)
        rc = sqlite3_bind_text(
            stmt, 6, raw.c_str(),
            static_cast<int>(raw.size()), SQLITE_TRANSIENT);

    if (rc == SQLITE_OK) {
        rc = sqlite3_step(stmt);
        if (rc == SQLITE_DONE)
            rc = SQLITE_OK;
    }

    if (rc != SQLITE_OK)
        error = sqlite3_errmsg(db);

    const int insert_finalize =
        stmt ? sqlite3_finalize(stmt) : SQLITE_OK;
    stmt = nullptr;

    if (rc == SQLITE_OK && insert_finalize != SQLITE_OK) {
        rc = insert_finalize;
        error = sqlite3_errmsg(db);
    }

    if (rc != SQLITE_OK) {
        std::string ignored;
        exec_sql(db, "ROLLBACK;", ignored);
        return false;
    }

    // 检查该消息ID对应的原始内容，防止同ID不同内容
    const char* select_sql =
        "SELECT payload FROM received_messages "
        "WHERE message_id = ?1;";

    rc = sqlite3_prepare_v2(
        db, select_sql, -1, &stmt, nullptr
    );

    if (rc == SQLITE_OK)
        rc = sqlite3_bind_text(
            stmt, 1, message_id.c_str(), -1, SQLITE_TRANSIENT);

    std::string stored_payload;

    if (rc == SQLITE_OK) {
        rc = sqlite3_step(stmt);

        if (rc == SQLITE_ROW) {
            const auto* data = sqlite3_column_text(stmt, 0);
            const int bytes = sqlite3_column_bytes(stmt, 0);

            if (data != nullptr) {
                stored_payload.assign(
                    reinterpret_cast<const char*>(data), bytes);
            }

            rc = SQLITE_OK;
        } else if (rc == SQLITE_DONE) {
            error = "message_id not found after insert";
            rc = SQLITE_ERROR;
        }
    }

    if (rc != SQLITE_OK && error.empty())
        error = sqlite3_errmsg(db);

    const int select_finalize =
        stmt ? sqlite3_finalize(stmt) : SQLITE_OK;

    if (rc == SQLITE_OK && select_finalize != SQLITE_OK) {
        rc = select_finalize;
        error = sqlite3_errmsg(db);
    }

    if (rc != SQLITE_OK) {
        std::string ignored;
        exec_sql(db, "ROLLBACK;", ignored);
        return false;
    }

    if (stored_payload != raw) {
        error = "message_id reused with different payload";
        std::string ignored;
        exec_sql(db, "ROLLBACK;", ignored);
        return false;
    }

    if (!exec_sql(db, "COMMIT;", error)) {
        std::string ignored;
        exec_sql(db, "ROLLBACK;", ignored);
        return false;
    }

    return true;
}

void handle_client(int client_fd,sqlite3* db,const std::string& drop_ack_id,
    bool& ack_dropped)
{
    
std::string envelope;

if (!tcp::recv_message(client_fd, envelope)) {
    std::cerr << "[center] receive failed\n";
    close(client_fd);
    return;
}

const size_t split = envelope.find('\n');

if (split == std::string::npos ||
    split == 0 ||
    split + 1 >= envelope.size()) {
    tcp::send_message(client_fd, "REJECTED: invalid envelope");
    close(client_fd);
    return;
}

const std::string message_id = envelope.substr(0, split);
const std::string raw = envelope.substr(split + 1);

DeviceMessage msg;
std::string error;

if (!parse_device_message(raw, msg, error)) {
    tcp::send_message(client_fd, "REJECTED: " + error);
    close(client_fd);
    return;
}

if (!persist_center_message(db, message_id, msg, raw, error)) {
    std::cerr << "[center] persist failed: "
              << error << '\n';

    tcp::send_message(client_fd, "CENTER_STORE_FAILED");
    close(client_fd);
    return;
}
// 仅用于故障注入测试
if (!ack_dropped &&
    !drop_ack_id.empty() &&
    message_id == drop_ack_id)
{
    ack_dropped = true;

    std::cerr << "[FAULT] ACK dropped for "
              << message_id << '\n';

    close(client_fd);
    return;
}
std::cout << "[center] stored message_id="
          << message_id
          << " device=" << msg.device_id
          << " seq=" << msg.sequence << '\n';

// 只有事务提交成功后才发送确认
tcp::send_message(client_fd, "CENTER_STORED");

close(client_fd);

}
}

int main()
{
const char* env = std::getenv("CENTER_DROP_ACK_ID");

const std::string drop_ack_id =
    env ? env : "";

bool ack_dropped = false;
    const int server_fd = socket(AF_INET, SOCK_STREAM, 0);
	sqlite3* db = nullptr;
std::string error;

if (!init_center_db(db, error)) {
    std::cerr << "[center] SQLite init failed: "
              << error << '\n';

    if (db)
        sqlite3_close(db);

    return 1;
}
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
    addr.sin_port = htons(kCenterPort);
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

    std::cout << "[center] listening on 127.0.0.1:"
              << kCenterPort << '\n';

    while (true) {
        sockaddr_in peer{};
        socklen_t peer_len = sizeof(peer);

        const int client_fd = accept(
            server_fd,
            reinterpret_cast<sockaddr*>(&peer),
            &peer_len
        );

        if (client_fd < 0) {
            if (errno == EINTR) {
                continue;
            }

            std::cerr << "accept failed: "
                      << std::strerror(errno) << '\n';
            continue;
        }

        handle_client(client_fd,db, drop_ack_id, ack_dropped);
    }

    close(server_fd);
    return 0;
}
