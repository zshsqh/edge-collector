
#pragma once
#include <mutex>
#include "device_message.h"
#include <vector>
#include <sqlite3.h>
#include <string>
struct PendingMessage {
    sqlite3_int64 id;
    std::string payload;
};
class LocalStore {
public:
bool load_pending_after(sqlite3_int64 after_id,
    std::vector<PendingMessage>& messages,
    std::string& error
);
bool mark_quarantined(
    sqlite3_int64 row_id,
    const std::string& reason,
    std::string& error
);  
  LocalStore() = default;
    ~LocalStore();

    // 避免多个对象意外共享同一个数据库指针并重复关闭
    LocalStore(const LocalStore&) = delete;
    LocalStore& operator=(const LocalStore&) = delete;

    bool open(const std::string& db_path, std::string& error);

    // 只有事务 COMMIT 成功，才返回 true
    
bool append(
    const DeviceMessage& msg,
    const std::string& raw,
    sqlite3_int64& row_id,
    std::string& error
);

bool mark_delivered(
    sqlite3_int64 row_id,
    std::string& error
);

private:
    bool exec(const char* sql, std::string& error);
std::mutex db_mutex_;
    sqlite3* db_ = nullptr;
};
