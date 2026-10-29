#include <cmath>
#include "device_message.h"

#include <iostream>

int main()
{
    int failures = 0;

    auto check = [&](const char* name, bool passed) {
        std::cout << (passed ? "[PASS] " : "[FAIL] ")
                  << name << '\n';

        if (!passed) {
            ++failures;
        }
    };

    DeviceMessage msg;
    std::string error;

    check("valid message",
        parse_device_message(
            "device01|1|1792200000000|25.6",
            msg, error
        ) &&
        msg.device_id == "device01" &&
        msg.sequence == 1 &&
        msg.timestamp_ms == 1792200000000LL &&
        std::abs(msg.temperature_c - 25.6) < 1e-9
    );

    check("missing field",
        !parse_device_message(
            "device01|1|1792200000000",
            msg, error
        )
    );

    check("extra field",
        !parse_device_message(
            "device01|1|1792200000000|25.6|extra",
            msg, error
        )
    );

    check("empty device id",
        !parse_device_message(
            "|1|1792200000000|25.6",
            msg, error
        )
    );

    check("zero sequence",
        !parse_device_message(
            "device01|0|1792200000000|25.6",
            msg, error
        )
    );

    check("invalid timestamp",
        !parse_device_message(
            "device01|1|wrong_time|25.6",
            msg, error
        )
    );

    check("invalid temperature",
        !parse_device_message(
            "device01|1|1792200000000|hello",
            msg, error
        )
    );

    check("non-finite temperature",
        !parse_device_message(
            "device01|1|1792200000000|nan",
            msg, error
        )
    );

    std::cout << "\nFailures: " << failures << '\n';
    return failures == 0 ? 0 : 1;
}
