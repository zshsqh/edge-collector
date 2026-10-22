
#pragma once

#include <array>
#include <cerrno>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <string_view>
#include <system_error>

struct DeviceMessage {
    std::string device_id;
    uint64_t sequence = 0;
    int64_t timestamp_ms = 0;
    double temperature_c = 0.0;
};

inline bool parse_device_message(
    const std::string& raw,
    DeviceMessage& out,
    std::string& error)
{
    std::array<std::string_view, 4> fields{};

    // 1. 必须恰好有4个字段
    size_t begin = 0;

    for (size_t i = 0; i < 3; ++i) {
        const size_t pos = raw.find('|', begin);

        if (pos == std::string::npos) {
            error = "expected exactly four fields";
            return false;
        }

        fields[i] = std::string_view(raw).substr(
            begin, pos - begin
        );
        begin = pos + 1;
    }

    if (raw.find('|', begin) != std::string::npos) {
        error = "too many fields";
        return false;
    }

    fields[3] = std::string_view(raw).substr(begin);

    for (const auto field : fields) {
        if (field.empty()) {
            error = "empty field";
            return false;
        }
    }

    // 2. 校验设备编号：1～32位，只允许字母、数字、_、-
    if (fields[0].size() > 32) {
        error = "device id too long";
        return false;
    }

    for (const char c : fields[0]) {
        const bool valid =
            (c >= 'a' && c <= 'z') ||
            (c >= 'A' && c <= 'Z') ||
            (c >= '0' && c <= '9') ||
            c == '_' || c == '-';

        if (!valid) {
            error = "invalid device id";
            return false;
        }
    }

    // 3. 解析消息序号
    uint64_t sequence = 0;

    const auto seq_result = std::from_chars(
        fields[1].data(),
        fields[1].data() + fields[1].size(),
        sequence
    );

    if (seq_result.ec != std::errc{} ||
        seq_result.ptr != fields[1].data() + fields[1].size() ||
        sequence == 0) {
        error = "invalid sequence";
        return false;
    }

    // 4. 解析 Unix 毫秒时间戳
    int64_t timestamp_ms = 0;

    const auto time_result = std::from_chars(
        fields[2].data(),
        fields[2].data() + fields[2].size(),
        timestamp_ms
    );

    if (time_result.ec != std::errc{} ||
        time_result.ptr != fields[2].data() + fields[2].size() ||
        timestamp_ms <= 0) {
        error = "invalid timestamp";
        return false;
    }

    // 5. 解析温度数值
    const std::string temp_text(fields[3]);

    if (temp_text.find_first_of(" \t\r\n") != std::string::npos) {
        error = "whitespace in temperature";
        return false;
    }

    errno = 0;
    char* end = nullptr;
    const double temperature_c =
        std::strtod(temp_text.c_str(), &end);

    if (errno == ERANGE ||
        end == temp_text.c_str() ||
        end != temp_text.c_str() + temp_text.size() ||
        !std::isfinite(temperature_c)) {
        error = "invalid temperature";
        return false;
    }

    // 全部校验通过后，才更新输出对象
    DeviceMessage parsed;
    parsed.device_id = std::string(fields[0]);
    parsed.sequence = sequence;
    parsed.timestamp_ms = timestamp_ms;
    parsed.temperature_c = temperature_c;

    out = std::move(parsed);
    error.clear();

    return true;
}
