#include "SceTypes.hpp"
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <thread>

#include "prx/libkernel/SyncOnAddress/src/SyncOnAddress.cpp"

extern "C" {
int APS5_VABI scePthreadCreate(Pthread*, const PthreadAttr*, PthreadEntry, void*, const char*);
int APS5_VABI scePthreadJoin(Pthread, void**);
Pthread APS5_VABI scePthreadSelf();
int APS5_VABI sceKernelInstallExceptionHandler(int, void*);
int APS5_VABI sceKernelRemoveExceptionHandler(int);
int APS5_VABI sceKernelRaiseException(Pthread, int);
}

static constexpr int Signal = 30;
static constexpr auto Deadline = std::chrono::seconds(5);
static void Check(bool value, int line) {
    if (!value) {
        std::fprintf(stderr, "Protected exception check failed at line %d\n", line);
        std::abort();
    }
}
#define Require(value) Check((value), __LINE__)

static std::atomic<int> entries{0};
static std::atomic<int> completed{0};
static std::atomic<bool> protectedPhase{false};
static std::atomic<bool> unsafeEntry{false};
static std::atomic<bool> nestedRaise{false};
static std::atomic<Pthread> expectedThread{nullptr};
static std::uint32_t handlerWord = 0;
static std::atomic<std::uint32_t*> handlerWakeWord{&handlerWord};

template <class TPredicate>
static void Await(TPredicate predicate) {
    const auto deadline = std::chrono::steady_clock::now() + Deadline;
    while (!predicate()) {
        Require(std::chrono::steady_clock::now() < deadline);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

static void APS5_VABI Handler(int signal, void*) {
    Require(signal == Signal);
    Require(scePthreadSelf() == expectedThread.load());
    entries.fetch_add(1);
    if (protectedPhase.load()) {
        unsafeEntry.store(true);
        return;
    }
    Require(sceKernelSyncOnAddressWake(handlerWakeWord.load(), 1) == 0);
    std::uint32_t nestedWord = 0;
    const KernelUseconds timeout = 1000;
    Require(sceKernelSyncOnAddressWait(&nestedWord, 0, &timeout, "handler wait") ==
            static_cast<int>(0x8002003c));
    completed.fetch_add(1);
    if (nestedRaise.exchange(false)) {
        Require(sceKernelRaiseException(scePthreadSelf(), Signal) == 0);
    }
}

struct Worker {
    std::atomic<bool> locked{false};
    std::atomic<bool> release{false};
    std::atomic<bool> returned{false};
    int wanted = 0;
};

static void* APS5_VABI LockedWorker(void* opaque) {
    auto& worker = *static_cast<Worker*>(opaque);
    expectedThread.store(scePthreadSelf());
    {
        std::lock_guard lock(g_waitersLock);
        protectedPhase.store(true);
        worker.locked.store(true);
        while (!worker.release.load()) std::this_thread::yield();
        protectedPhase.store(false);
    }
    Await([&] { return completed.load() == worker.wanted; });
    worker.returned.store(true);
    return nullptr;
}

static void ForeignRaisesWaitForUnlock() {
    Worker worker;
    worker.wanted = 4;
    nestedRaise.store(true);
    Pthread thread = nullptr;
    Require(scePthreadCreate(&thread, nullptr, LockedWorker, &worker, "protected raise") == 0);
    Await([&] { return worker.locked.load(); });
    Require(sceKernelRaiseException(thread, Signal) == 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    Require(entries.load() == 0);
    Require(!unsafeEntry.load());
    Require(sceKernelRaiseException(thread, Signal) == 0);
    Require(sceKernelRaiseException(thread, Signal) == 0);
    Require(entries.load() == 0);
    worker.release.store(true);
    Await([&] { return worker.returned.load(); });
    Require(scePthreadJoin(thread, nullptr) == 0);
    Require(entries.load() == worker.wanted);
    Require(completed.load() == worker.wanted);
    Require(!unsafeEntry.load());
}

static void SelfRaiseWaitsForOutermostUnlock() {
    expectedThread.store(scePthreadSelf());
    const int before = completed.load();
    decltype(g_waitersLock) innerMutex;
    {
        std::lock_guard outer(g_waitersLock);
        protectedPhase.store(true);
        Require(sceKernelRaiseException(scePthreadSelf(), Signal) == 0);
        Require(completed.load() == before);
        {
            std::lock_guard inner(innerMutex);
            Require(sceKernelRaiseException(scePthreadSelf(), Signal) == 0);
            Require(completed.load() == before);
        }
        Require(completed.load() == before);
        Require(!unsafeEntry.load());
        protectedPhase.store(false);
    }
    Await([&] { return completed.load() == before + 2; });
    Require(!unsafeEntry.load());
}

struct AddressWorker {
    std::atomic<bool> started{false};
    std::atomic<bool> returned{false};
    std::uint32_t word = 0;
    int result = -1;
};

static void* APS5_VABI WaitingWorker(void* opaque) {
    auto& worker = *static_cast<AddressWorker*>(opaque);
    expectedThread.store(scePthreadSelf());
    worker.started.store(true);
    const KernelUseconds timeout = 2000000;
    worker.result = sceKernelSyncOnAddressWait(&worker.word, 0, &timeout, "protected wait");
    worker.returned.store(true);
    return nullptr;
}

static void HandlerCanReenterAddressWake() {
    AddressWorker worker;
    Pthread thread = nullptr;
    const int before = completed.load();
    Require(scePthreadCreate(&thread, nullptr, WaitingWorker, &worker, "address raise") == 0);
    Await([&] { return worker.started.load(); });
    Await([&] {
        std::lock_guard lock(g_waitersLock);
        return g_waiters.find(reinterpret_cast<std::uintptr_t>(&worker.word)) != g_waiters.end();
    });
    Require(sceKernelRaiseException(thread, Signal) == 0);
    Await([&] { return completed.load() == before + 1; });
    Require(sceKernelSyncOnAddressWake(&worker.word, 1) == 0);
    Await([&] { return worker.returned.load(); });
    Require(scePthreadJoin(thread, nullptr) == 0);
    Require(worker.result == 0);
    Require(!unsafeEntry.load());
}

struct CoarseWorker {
    std::atomic<bool> inserted{false};
    std::atomic<bool> enterWait{false};
    std::atomic<bool> returned{false};
    std::uint32_t word = 0;
    bool timed = false;
};

static void* APS5_VABI CoarseWaitingWorker(void* opaque) {
    auto& worker = *static_cast<CoarseWorker*>(opaque);
    expectedThread.store(scePthreadSelf());
    AddressWaiter waiter;
    std::unique_lock lock(g_waitersLock);
    const auto key = reinterpret_cast<std::uintptr_t>(&worker.word);
    g_waiters[key].push_back(&waiter);
    protectedPhase.store(true);
    worker.inserted.store(true);
    while (!worker.enterWait.load()) std::this_thread::yield();
    const auto isWoken = [&] { return waiter.woken; };
    if (worker.timed) {
        Require(waiter.condition.WaitUntil(lock, TimedWait::DeadlineNanos(2000000), isWoken));
    } else {
        waiter.condition.Wait(lock, isWoken);
    }
    Require(waiter.woken);
    protectedPhase.store(false);
    lock.unlock();
    worker.returned.store(true);
    return nullptr;
}

static void CoarseWaitKeepsInternalLocksProtected(bool timed) {
    Require(TimedWait::Coarse());
    CoarseWorker worker;
    worker.timed = timed;
    handlerWakeWord.store(&worker.word);
    const int before = completed.load();
    const int entriesBefore = entries.load();
    Pthread thread = nullptr;
    Require(scePthreadCreate(&thread, nullptr, CoarseWaitingWorker, &worker, "coarse raise") == 0);
    Await([&] { return worker.inserted.load(); });
    Require(sceKernelRaiseException(thread, Signal) == 0);
    Require(entries.load() == entriesBefore);
    worker.enterWait.store(true);
    Require(sceKernelSyncOnAddressWake(&worker.word, 1) == 0);
    Await([&] { return worker.returned.load(); });
    Require(scePthreadJoin(thread, nullptr) == 0);
    Require(!unsafeEntry.load());
    Require(entries.load() == entriesBefore + 1);
    Require(completed.load() == before + 1);
    handlerWakeWord.store(&handlerWord);
}

static void ScopeUnwindingRestoresDelivery() {
    expectedThread.store(scePthreadSelf());
    const int before = completed.load();
    bool caught = false;
    try {
        ExceptionDelivery::Scope outer;
        protectedPhase.store(true);
        {
            ExceptionDelivery::Scope inner;
            Require(sceKernelRaiseException(scePthreadSelf(), Signal) == 0);
        }
        Require(completed.load() == before);
        protectedPhase.store(false);
        throw 17;
    } catch (int value) {
        Require(value == 17);
        caught = true;
    }
    Require(caught);
    Require(!unsafeEntry.load());
    Require(completed.load() == before + 1);
    Require(sceKernelRaiseException(scePthreadSelf(), Signal) == 0);
    Require(completed.load() == before + 2);
}

int main(int argc, char**) {
    Require(sceKernelInstallExceptionHandler(Signal, reinterpret_cast<void*>(&Handler)) == 0);
    ForeignRaisesWaitForUnlock();
    SelfRaiseWaitsForOutermostUnlock();
    if (argc > 1) {
        CoarseWaitKeepsInternalLocksProtected(false);
        CoarseWaitKeepsInternalLocksProtected(true);
    } else {
        HandlerCanReenterAddressWake();
    }
    ScopeUnwindingRestoresDelivery();
    Require(sceKernelRemoveExceptionHandler(Signal) == 0);
}
