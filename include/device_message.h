#pragma once
#include <cerrno>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

struct DeviceMessage {
    std::string device_id;
    std::string event_id;
    uint64_t sequence = 0;
    int64_t timestamp_ms = 0;
    double temperature_c = 0.0;
};

inline bool parse_device_message(const std::string& raw,
                                 DeviceMessage& out, std::string& error) {
    std::vector<std::string_view> f;
    size_t start = 0;
    while (true) {
        if (f.size() == 5) { error = "too many fields"; return false; }
        size_t end = raw.find('|', start);
        f.emplace_back(raw.data() + start,
                       (end == std::string::npos ? raw.size() : end) - start);
        if (end == std::string::npos) break;
        start = end + 1;
    }
    if (f.size() != 4 && f.size() != 5) { error = "expected 4 or 5 fields"; return false; }
    for (auto s : f) if (s.empty()) { error = "empty field"; return false; }
    const bool v2 = f.size() == 5;
    const auto seq_text = f[v2 ? 2 : 1];
    const auto time_text = f[v2 ? 3 : 2];
    const auto temp_text = f[v2 ? 4 : 3];
    auto valid_id = [](std::string_view s, size_t limit) {
        if (s.empty() || s.size() > limit) return false;
        for (char c : s) {
            if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                  (c >= '0' && c <= '9') || c == '_' || c == '-')) return false;
        }
        return true;
    };
    if (!valid_id(f[0], 32)) { error = "invalid device id"; return false; }
    if (v2 && !valid_id(f[1], 64)) { error = "invalid event id"; return false; }
    uint64_t seq = 0;
    auto rs = std::from_chars(seq_text.data(), seq_text.data() + seq_text.size(), seq);
    if (rs.ec != std::errc{} || rs.ptr != seq_text.data() + seq_text.size() || seq == 0) {
        error = "invalid sequence"; return false;
    }
    int64_t ts = 0;
    auto rt = std::from_chars(time_text.data(), time_text.data() + time_text.size(), ts);
    if (rt.ec != std::errc{} || rt.ptr != time_text.data() + time_text.size() || ts <= 0) {
        error = "invalid timestamp"; return false;
    }
    const std::string temp_str(temp_text);
    if (temp_str.find_first_of(" \t\r\n") != std::string::npos) {
        error = "whitespace in temperature"; return false;
    }
    errno = 0;
    char* end = nullptr;
    double temperature = std::strtod(temp_str.c_str(), &end);
    if (errno == ERANGE || end == temp_str.c_str() ||
        end != temp_str.c_str() + temp_str.size() || !std::isfinite(temperature)) {
        error = "invalid temperature"; return false;
    }
    DeviceMessage parsed;
    parsed.device_id = std::string(f[0]);
    parsed.event_id = v2 ? std::string(f[1]) : "";
    parsed.sequence = seq;
    parsed.timestamp_ms = ts;
    parsed.temperature_c = temperature;
    out = std::move(parsed);
    error.clear();
    return true;
}
