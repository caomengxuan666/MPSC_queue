#include <benchmark/benchmark.h>

#include <chrono>
#include <atomic>
#include <cstddef>
#include <string>
#include <thread>
#include <vector>

#include "daking/MPSC_queue.hpp"

namespace {

using TestQueue = daking::MPSC_queue<int>;

constexpr std::size_t kItemsPerProducer = 200000;

void producer_thread(TestQueue* q, std::size_t items_to_push, std::atomic_bool* start) {
    while (!start->load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    for (std::size_t i = 0; i < items_to_push; ++i) {
        q->enqueue(1);
    }
}

void consumer_thread(TestQueue* q, std::size_t total_items_to_pop, std::atomic_bool* start) {
    while (!start->load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    std::size_t popped_count = 0;
    int val = 0;
    while (popped_count < total_items_to_pop) {
        if (q->try_dequeue(val)) {
            ++popped_count;
        }
        else {
            std::this_thread::yield();
        }
    }
}

void run_burst(TestQueue& q, int producers, std::size_t items_per_producer) {
    const std::size_t total_items = items_per_producer * static_cast<std::size_t>(producers);
    std::atomic_bool start{false};

    std::thread consumer(consumer_thread, &q, total_items, &start);

    std::vector<std::thread> producer_threads;
    producer_threads.reserve(static_cast<std::size_t>(producers));
    for (int i = 0; i < producers; ++i) {
        producer_threads.emplace_back(producer_thread, &q, items_per_producer, &start);
    }

    start.store(true, std::memory_order_release);
    for (auto& producer : producer_threads) {
        producer.join();
    }
    consumer.join();
}

template <bool ReclaimBetweenBursts>
static void BM_MPSC_MemoryCycle(benchmark::State& state) {
    const int producers = static_cast<int>(state.range(0));
    const std::size_t items_per_producer = kItemsPerProducer;
    const std::size_t total_items_per_burst = items_per_producer * static_cast<std::size_t>(producers);

    for (auto _ : state) {
        TestQueue q;
        TestQueue::reserve_global_chunk(64);

        run_burst(q, producers, items_per_producer);
        if (!q.empty()) {
            state.SkipWithError("Queue should be empty after first burst");
            break;
        }

        const auto nodes_before = TestQueue::global_node_size_apprx();
        double shrink_us = 0.0;
        if constexpr (ReclaimBetweenBursts) {
            const auto shrink_start = std::chrono::steady_clock::now();
            if (!q.shrink_to_fit()) {
                state.SkipWithError("shrink_to_fit failed unexpectedly");
                break;
            }
            const auto shrink_end = std::chrono::steady_clock::now();
            shrink_us = std::chrono::duration<double, std::micro>(shrink_end - shrink_start).count();
        }
        const auto nodes_after = TestQueue::global_node_size_apprx();

        run_burst(q, producers, items_per_producer);
        if (!q.empty()) {
            state.SkipWithError("Queue should be empty after second burst");
            break;
        }

        state.counters["global_nodes_before"] = static_cast<double>(nodes_before);
        state.counters["global_nodes_after"] = static_cast<double>(nodes_after);
        if constexpr (ReclaimBetweenBursts) {
            state.counters["shrink_us"] = shrink_us;
        }
    }

    state.SetItemsProcessed(static_cast<int64_t>(total_items_per_burst * 2 * state.iterations()));
    state.SetLabel(std::string(ReclaimBetweenBursts ? "idle_reclaim" : "stable") + ", P=" + std::to_string(producers));
}

} // namespace

BENCHMARK_TEMPLATE(BM_MPSC_MemoryCycle, false)->Arg(1)->Arg(4)->Arg(16)->UseRealTime()->MinWarmUpTime(1.0);
BENCHMARK_TEMPLATE(BM_MPSC_MemoryCycle, true)->Arg(1)->Arg(4)->Arg(16)->UseRealTime()->MinWarmUpTime(1.0);

BENCHMARK_MAIN();
