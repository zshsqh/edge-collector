
#include "local_store.h"
#include <utility>

// 1. 析构：关闭数据库
LocalStore::~LocalStore()
{
    // 补传线程必须先停止并 join
    if (db_ != nullptr) {
        sqlite3_close(db_);
        db_ = nullptr;
    }
}

// 2. 执行普通 SQL
bool LocalStore::exec(
    const char* sql,
    std::string& error)
{
    char* detail = nullptr;

    const int rc = sqlite3_exec(
        db_, sql, nullptr, nullptr, &detail);

    if (rc != SQLITE_OK) {
        error = detail ? detail : sqlite3_errmsg(db_);
        sqlite3_free(detail);
        return false;
    }

    return true;
}

// 3. 打开数据库、建表和升级旧表
bool LocalStore::open(
    const std::string& db_path,
    std::string& error)
{
    error.clear();

    if (db_ != nullptr) {
        error = "database already open";
        return false;
    }

    if (sqlite3_open(db_path.c_str(), &db_) != SQLITE_OK) {
        error = db_ ? sqlite3_errmsg(db_)
                    : "sqlite3_open failed";

        if (db_ != nullptr)
            sqlite3_close(db_);

        db_ = nullptr;
        return false;
    }

    const auto fail = [&]() {
        sqlite3_close(db_);
        db_ = nullptr;
        return false;
    };

    if (sqlite3_busy_timeout(db_, 2000) != SQLITE_OK) {
        error = sqlite3_errmsg(db_);
        return fail();
    }

    if (!exec("PRAGMA journal_mode=WAL;", error) ||
        !exec("PRAGMA synchronous=FULL;", error)) {
        return fail();
    }

    if (!exec(
        "CREATE TABLE IF NOT EXISTS outbox ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "device_id TEXT NOT NULL,"
        "event_id TEXT,"
        "seq TEXT NOT NULL,"
        "timestamp_ms INTEGER NOT NULL,"
        "temperature_c REAL NOT NULL,"
        "payload TEXT NOT NULL,"
        "delivered INTEGER NOT NULL DEFAULT 0 "
        "CHECK(delivered IN (0,1)),"
        "created_at TEXT NOT NULL "
        "DEFAULT CURRENT_TIMESTAMP"
        ");", error)) {
        return fail();
    }

    // 判断旧数据库是否已有 event_id 列
    bool found_event_id = false;
    sqlite3_stmt* stmt = nullptr;

    int rc = sqlite3_prepare_v2(
        db_,
        "PRAGMA table_info(outbox);",
        -1, &stmt, nullptr);

    if (rc != SQLITE_OK) {
        error = sqlite3_errmsg(db_);

        if (stmt)
            sqlite3_finalize(stmt);

        return fail();
    }

    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        const unsigned char* name =
            sqlite3_column_text(stmt, 1);

        if (name &&
            std::string(
                reinterpret_cast<const char*>(name)
            ) == "event_id") {
            found_event_id = true;
        }
    }

    if (rc != SQLITE_DONE)
        error = sqlite3_errmsg(db_);

    const int finish_rc = sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE || finish_rc != SQLITE_OK) {
        if (error.empty())
            error = sqlite3_errmsg(db_);

        return fail();
    }

    // 旧表没有 event_id 时自动增加
    if (!found_event_id &&
        !exec(
            "ALTER TABLE outbox "
            "ADD COLUMN event_id TEXT;",
            error)) {
        return fail();
    }

    // 设备ID和事件ID联合唯一
    if (!exec(
        "CREATE UNIQUE INDEX IF NOT EXISTS "
        "idx_outbox_device_event "
        "ON outbox(device_id, event_id);",
        error)) {
        return fail();
    }
if (!exec(
    "CREATE TABLE IF NOT EXISTS outbox_quarantine ("
    "outbox_id INTEGER PRIMARY KEY,"
    "reason TEXT NOT NULL,"
    "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP"
    ");",
    error)) {
    return fail();
}
    return true;
}

// 4. 保存消息；重复事件复用原记录
bool LocalStore::append(
    const DeviceMessage& msg,
    const std::string& raw,
    sqlite3_int64& row_id,
    std::string& error)
{
    std::lock_guard<std::mutex> lock(db_mutex_);

    error.clear();
    row_id = 0;

    if (!db_) {
        error = "database not open";
        return false;
    }

    if (msg.event_id.empty()) {
        error = "event_id required";
        return false;
    }

    if (!exec("BEGIN IMMEDIATE;", error))
        return false;

    const auto rollback = [&]() {
        std::string rollback_error;

        if (!exec("ROLLBACK;", rollback_error)) {
            error += "; ROLLBACK failed: " + rollback_error;
        }

        return false;
    };

    const char* insert_sql =
        "INSERT INTO outbox("
        "device_id,event_id,seq,timestamp_ms,"
        "temperature_c,payload"
        ") VALUES(?1,?2,?3,?4,?5,?6) "
        "ON CONFLICT(device_id,event_id) DO NOTHING;";

    sqlite3_stmt* stmt = nullptr;

    int rc = sqlite3_prepare_v2(
        db_, insert_sql, -1, &stmt, nullptr);

    const std::string seq =
        std::to_string(msg.sequence);

    if (rc == SQLITE_OK)
        rc = sqlite3_bind_text(
            stmt, 1, msg.device_id.c_str(),
            -1, SQLITE_TRANSIENT);

    if (rc == SQLITE_OK)
        rc = sqlite3_bind_text(
            stmt, 2, msg.event_id.c_str(),
            -1, SQLITE_TRANSIENT);

    if (rc == SQLITE_OK)
        rc = sqlite3_bind_text(
            stmt, 3, seq.c_str(),
            -1, SQLITE_TRANSIENT);

    if (rc == SQLITE_OK)
        rc = sqlite3_bind_int64(
            stmt, 4, msg.timestamp_ms);

    if (rc == SQLITE_OK)
        rc = sqlite3_bind_double(
            stmt, 5, msg.temperature_c);

    if (rc == SQLITE_OK)
        rc = sqlite3_bind_text(
            stmt, 6, raw.data(),
            static_cast<int>(raw.size()),
            SQLITE_TRANSIENT);

    if (rc == SQLITE_OK)
        rc = sqlite3_step(stmt);

    if (rc != SQLITE_DONE)
        error = sqlite3_errmsg(db_);

    const int insert_fin =
        stmt ? sqlite3_finalize(stmt) : SQLITE_OK;

    if (rc != SQLITE_DONE ||
        insert_fin != SQLITE_OK) {
        if (error.empty())
            error = sqlite3_errmsg(db_);

        return rollback();
    }

    // 查询原有记录，确认相同ID的数据内容一致
    const char* select_sql =
        "SELECT id,payload FROM outbox "
        "WHERE device_id=?1 AND event_id=?2;";

    stmt = nullptr;

    rc = sqlite3_prepare_v2(
        db_, select_sql, -1, &stmt, nullptr);

    if (rc == SQLITE_OK)
        rc = sqlite3_bind_text(
            stmt, 1, msg.device_id.c_str(),
            -1, SQLITE_TRANSIENT);

    if (rc == SQLITE_OK)
        rc = sqlite3_bind_text(
            stmt, 2, msg.event_id.c_str(),
            -1, SQLITE_TRANSIENT);

    sqlite3_int64 existing_id = 0;
    std::string existing_payload;

    if (rc == SQLITE_OK) {
        rc = sqlite3_step(stmt);

        if (rc == SQLITE_ROW) {
            existing_id =
                sqlite3_column_int64(stmt, 0);

            const unsigned char* ptr =
                sqlite3_column_text(stmt, 1);

            const int len =
                sqlite3_column_bytes(stmt, 1);

            if (ptr != nullptr) {
                existing_payload.assign(
                    reinterpret_cast<const char*>(ptr),
                    len);
            }
        }
    }

    if (rc != SQLITE_ROW)
        error = sqlite3_errmsg(db_);

    const int select_fin =
        stmt ? sqlite3_finalize(stmt) : SQLITE_OK;

    if (rc != SQLITE_ROW ||
        select_fin != SQLITE_OK) {
        if (error.empty())
            error = sqlite3_errmsg(db_);

        return rollback();
    }

    // 同ID不同内容：拒绝
    if (existing_payload != raw) {
        error = "same event_id with different payload";
        return rollback();
    }

    if (!exec("COMMIT;", error))
        return rollback();

    // 返回新记录或旧记录的真实ID
    row_id = existing_id;
    return true;
}

// 5. 中心确认后，更新 delivered
bool LocalStore::mark_delivered(
    sqlite3_int64 row_id,
    std::string& error)
{
    std::lock_guard<std::mutex> lock(db_mutex_);

    error.clear();

    if (!db_) {
        error = "database not open";
        return false;
    }

    sqlite3_stmt* stmt = nullptr;

    int rc = sqlite3_prepare_v2(
        db_,
        "UPDATE outbox SET delivered=1 WHERE id=?1;",
        -1, &stmt, nullptr);

    if (rc == SQLITE_OK)
        rc = sqlite3_bind_int64(stmt, 1, row_id);

    if (rc == SQLITE_OK)
        rc = sqlite3_step(stmt);

    if (rc != SQLITE_DONE)
        error = sqlite3_errmsg(db_);

    const int changed =
        rc == SQLITE_DONE ? sqlite3_changes(db_) : 0;

    const int fin =
        stmt ? sqlite3_finalize(stmt) : SQLITE_OK;

    if (rc != SQLITE_DONE || fin != SQLITE_OK) {
        if (error.empty())
            error = sqlite3_errmsg(db_);

        return false;
    }

    if (changed != 1) {
        error = "outbox row not found";
        return false;
    }

    return true;
}

// 6. 查询需要补传的记录
bool LocalStore::load_pending_after(
    sqlite3_int64 after_id,
    std::vector<PendingMessage>& messages,
    std::string& error)
{
    std::lock_guard<std::mutex> lock(db_mutex_);

    messages.clear();
    error.clear();

    if (!db_) {
        error = "database not open";
        return false;
    }

    sqlite3_stmt* stmt = nullptr;

    int rc = sqlite3_prepare_v2(
        db_,
        "SELECT o.id, o.payload FROM outbox AS o "
    "WHERE o.delivered = 0 "
    "AND o.id > ?1 "
    "AND NOT EXISTS ("
    "  SELECT 1 FROM outbox_quarantine AS q "
    "  WHERE q.outbox_id = o.id"
    ") "
    "ORDER BY o.id LIMIT 100;",
        -1, &stmt, nullptr);

    if (rc == SQLITE_OK)
        rc = sqlite3_bind_int64(stmt, 1, after_id);

    bool ok = (rc == SQLITE_OK);

    if (!ok)
        error = sqlite3_errmsg(db_);

    while (ok) {
        rc = sqlite3_step(stmt);

        if (rc == SQLITE_DONE)
            break;

        if (rc != SQLITE_ROW) {
            error = sqlite3_errmsg(db_);
            ok = false;
            break;
        }

        PendingMessage item;

        item.id = sqlite3_column_int64(stmt, 0);

        const unsigned char* ptr =
            sqlite3_column_text(stmt, 1);

        const int len =
            sqlite3_column_bytes(stmt, 1);

        if (!ptr) {
            error = "outbox payload is NULL";
            ok = false;
            break;
        }

        item.payload.assign(
            reinterpret_cast<const char*>(ptr),
            len);

        messages.push_back(std::move(item));
    }

    const int fin =
        stmt ? sqlite3_finalize(stmt) : SQLITE_OK;

    if (ok && fin != SQLITE_OK) {
        error = sqlite3_errmsg(db_);
        ok = false;
    }

    if (!ok)
        messages.clear();

    return ok;
}

bool LocalStore::mark_quarantined(
    sqlite3_int64 row_id,
    const std::string& reason,
    std::string& error)
{
    std::lock_guard<std::mutex> lock(db_mutex_);
    error.clear();

    if (db_ == nullptr) {
        error = "database not open";
        return false;
    }

    const char* sql =
        "INSERT INTO outbox_quarantine(outbox_id, reason) "
        "VALUES(?1, ?2) "
        "ON CONFLICT(outbox_id) DO UPDATE SET "
        "reason = excluded.reason;";

    sqlite3_stmt* stmt = nullptr;

    int rc = sqlite3_prepare_v2(
        db_, sql, -1, &stmt, nullptr
    );

    if (rc == SQLITE_OK) {
        rc = sqlite3_bind_int64(
            stmt, 1, row_id
        );
    }

    if (rc == SQLITE_OK) {
        rc = sqlite3_bind_text(
            stmt, 2, reason.c_str(),
            -1, SQLITE_TRANSIENT
        );
    }

    if (rc == SQLITE_OK) {
        rc = sqlite3_step(stmt);
    }

    bool success = (rc == SQLITE_DONE);

    if (!success) {
        error = sqlite3_errmsg(db_);
    }

    const int finalize_rc =
        stmt ? sqlite3_finalize(stmt) : SQLITE_OK;

    if (finalize_rc != SQLITE_OK) {
        if (error.empty())
            error = sqlite3_errmsg(db_);

        success = false;
    }

    return success;
}
