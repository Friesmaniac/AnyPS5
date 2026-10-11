#pragma once

#include <mutex>

namespace ExceptionDelivery {

#ifdef _WIN32
extern "C" void EnterExceptionRegion_nid_no_patch();
extern "C" void LeaveExceptionRegion_nid_no_patch();

class Scope {
public:
    Scope() { EnterExceptionRegion_nid_no_patch(); }
    ~Scope() { LeaveExceptionRegion_nid_no_patch(); }
    Scope(const Scope&) = delete;
    Scope& operator=(const Scope&) = delete;
};

class Mutex {
public:
    void lock() {
        EnterExceptionRegion_nid_no_patch();
        try {
            mutex.lock();
        } catch (...) {
            LeaveExceptionRegion_nid_no_patch();
            throw;
        }
    }

    void unlock() {
        mutex.unlock();
        LeaveExceptionRegion_nid_no_patch();
    }

private:
    std::mutex mutex;
};
#else
using Mutex = std::mutex;
#endif

}  // namespace ExceptionDelivery
