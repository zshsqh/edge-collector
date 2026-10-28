#include <mutex>
#include "local_store.h"

#include <string>

LocalStore::~LocalStore()
{
    if (db_ != nullptr) {
        sqlite3_close(db_);
        db_ = nullptr;
    }
}

// 执行不需要绑定参数的 SQL，并保存错误信息
bool LocalStore::exec(const char* sql, std::string& error)
{
    char* detail = nullptr;

    const int rc = sqlite3_exec(
        db_, sql, nullptr, nullptr, &detail
    );

    if (rc != SQLITE_OK) {
        error = detail ? detail : sqlite3_errmsg(db_);
        sqlite3_free(detail);
        return false;
    }

    return true;
}
bool LocalStore::load_pending_after(sqlite3_int64 after_id,
std::vector<PendingMessage>& messages,
    std::string& error
){
std::lock_guard<std::mutex> lock(db_mutex_);

    messages.clear();
    error.clear();

    if (db_ == nullptr) {
        error = "database not open";
        return false;
    }

    const char* sql =
        "SELECT id, payload FROM outbox "
        "WHERE delivered = 0 AND id > ?1 "
        "ORDER BY id LIMIT 100;";

    sqlite3_stmt* stmt = nullptr;

    int rc = sqlite3_prepare_v2(
        db_, sql, -1, &stmt, nullptr
    );

    if (rc == SQLITE_OK) {
        rc = sqlite3_bind_int64(stmt, 1, after_id);
    }

    bool success = (rc == SQLITE_OK);

    if (!success) {
        error = sqlite3_errmsg(db_);
    }

    while (success) {
        rc = sqlite3_step(stmt);

        if (rc == SQLITE_DONE) {
            break;
        }

        if (rc != SQLITE_ROW) {
            error = sqlite3_errmsg(db_);
            success = false;
            break;
        }

        PendingMessage item;
        item.id = sqlite3_column_int64(stmt, 0);

        const auto* text = sqlite3_column_text(stmt, 1);
        const int bytes = sqlite3_column_bytes(stmt, 1);

        if (text != nullptr && bytes > 0) {
            item.payload.assign(
                reinterpret_cast<const char*>(text),
                bytes
            );
        }

        messages.push_back(std::move(item));
    }

    const int finalize_rc =
        stmt ? sqlite3_finalize(stmt) : SQLITE_OK;

    if (success && finalize_rc != SQLITE_OK) {
        error = sqlite3_errmsg(db_);
        success = false;
    }

    return success;

}
bool LocalStore::open(
    const std::string& db_path,
    std::string& error)
{
    if (db_ != nullptr) {
        error = "database already open";
        return false;
    }

    const int rc = sqlite3_open(db_path.c_str(), &db_);

    if (rc != SQLITE_OK) {
        error = db_ ? sqlite3_errmsg(db_) : "sqlite open failed";
        return false;
    }

    // 如果数据库正被其他连接占用，最多等待约2秒
    if (sqlite3_busy_timeout(db_, 2000) != SQLITE_OK) {
        error = sqlite3_errmsg(db_);
        return false;
    }

    // WAL 日志模式
    if (!exec("PRAGMA journal_mode=WAL;", error)) {
        return false;
    }

    // 提高提交操作的持久性保障
    if (!exec("PRAGMA synchronous=FULL;", error)) {
        return false;
    }

    // 创建待传数据表
    return exec(
        "CREATE TABLE IF NOT EXISTS outbox ("
        " id INTEGER PRIMARY KEY AUTOINCREMENT,"
        " device_id TEXT NOT NULL,"
        " seq TEXT NOT NULL,"
        " timestamp_ms INTEGER NOT NULL,"
        " temperature_c REAL NOT NULL,"
        " payload TEXT NOT NULL,"
        " delivered INTEGER NOT NULL DEFAULT 0"
        " CHECK(delivered IN (0,1)),"
        " created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP"
        ");",
        error
    );
}

bool LocalStore::append(
    const DeviceMessage& msg,
    const std::string& raw,
sqlite3_int64& row_id,

    std::string& error)
{std::lock_guard<std::mutex> lock(db_mutex_);
    if (db_ == nullptr) {
        error = "database not open";
        return false;
    }

    // 1. 开始事务，尽早申请写事务
    if (!exec("BEGIN IMMEDIATE;", error)) {
        return false;
    }

    // 2. 准备 INSERT 语句
    const char* sql =
        "INSERT INTO outbox("
        "device_id, seq, timestamp_ms,"
        "temperature_c, payload"
        ") VALUES(?1, ?2, ?3, ?4, ?5);";

    sqlite3_stmt* stmt = nullptr;

    int rc = sqlite3_prepare_v2(
        db_, sql, -1, &stmt, nullptr
    );

    if (rc == SQLITE_OK) {
        const std::string seq = std::to_string(msg.sequence);

        // 3. 绑定参数
        rc = sqlite3_bind_text(
            stmt, 1, msg.device_id.c_str(),
            -1, SQLITE_TRANSIENT
        );

        if (rc == SQLITE_OK) {
            rc = sqlite3_bind_text(
                stmt, 2, seq.c_str(),
                -1, SQLITE_TRANSIENT
            );
        }

        if (rc == SQLITE_OK) {
            rc = sqlite3_bind_int64(
                stmt, 3, msg.timestamp_ms
            );
        }

        if (rc == SQLITE_OK) {
            rc = sqlite3_bind_double(
                stmt, 4, msg.temperature_c
            );
        }

        if (rc == SQLITE_OK) {
            rc = sqlite3_bind_text(
                stmt, 5, raw.c_str(),
                static_cast<int>(raw.size()),
                SQLITE_TRANSIENT
            );
        }

        // 4. 执行 INSERT
        if (rc == SQLITE_OK) {
            rc = sqlite3_step(stmt);

            if (rc == SQLITE_DONE) {
                rc = SQLITE_OK;
            }
        }
    }

    if (rc != SQLITE_OK) {
        error = sqlite3_errmsg(db_);
    }

    // 5. 释放预编译语句
    const int finalize_rc =
        stmt ? sqlite3_finalize(stmt) : SQLITE_OK;

    if (rc == SQLITE_OK && finalize_rc != SQLITE_OK) {
        rc = finalize_rc;
        error = sqlite3_errmsg(db_);
    }

    // 6. 前面的步骤失败，回滚事务
    if (rc != SQLITE_OK) {
        std::string rollback_error;
        exec("ROLLBACK;", rollback_error);
        return false;
    }

    // 7. 所有写入操作正常完成后，尝试提交事务
    if (!exec("COMMIT;", error)) {
        std::string rollback_error;
        exec("ROLLBACK;", rollback_error);
        return false;
    }
row_id = sqlite3_last_insert_rowid(db_);
    // 8. 只有 COMMIT 成功才返回 true
    return true;
}

bool LocalStore::mark_delivered(
    sqlite3_int64 row_id,
    std::string& error)
{std::lock_guard<std::mutex> lock(db_mutex_);
    if (db_ == nullptr) {
        error = "database not open";
        return false;
    }

    const char* sql =
        "UPDATE outbox SET delivered = 1 WHERE id = ?1;";

    sqlite3_stmt* stmt = nullptr;

    int rc = sqlite3_prepare_v2(
        db_, sql, -1, &stmt, nullptr
    );

    if (rc == SQLITE_OK) {
        rc = sqlite3_bind_int64(stmt, 1, row_id);
    }

    if (rc == SQLITE_OK) {
        rc = sqlite3_step(stmt);

        if (rc == SQLITE_DONE) {
            rc = SQLITE_OK;
        }
    }

    // 在释放语句前检查影响的记录数
    const int changed =
        rc == SQLITE_OK ? sqlite3_changes(db_) : 0;

    if (rc != SQLITE_OK) {
        error = sqlite3_errmsg(db_);
    }

    const int finalize_rc =
        stmt ? sqlite3_finalize(stmt) : SQLITE_OK;

    if (rc == SQLITE_OK && finalize_rc != SQLITE_OK) {
        rc = finalize_rc;
        error = sqlite3_errmsg(db_);
    }

    if (rc != SQLITE_OK) {
        return false;
    }

    if (changed != 1) {
        error = "outbox row not found";
        return false;
    }

    return true;
}
