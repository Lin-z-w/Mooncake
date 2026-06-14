#include "tenant_qos_scheduler.h"

#include <gtest/gtest.h>

#include <chrono>
#include <optional>
#include <string>
#include <vector>

namespace mooncake {

namespace {

using Scheduler = TenantQosScheduler;
using Request = TenantQosScheduler::Request;
using Clock = TenantQosScheduler::Clock;

Request MakeRequest(const std::string& tenant_id,
                    Scheduler::OperationType operation_type, uint64_t bytes,
                    Clock::time_point enqueue_time,
                    const std::string& object_key = "") {
    Request request;
    request.tenant_id = tenant_id;
    request.operation_type = operation_type;
    request.object_key = object_key;
    request.bytes = bytes;
    request.enqueue_time = enqueue_time;
    return request;
}

}  // namespace

TEST(TenantQosSchedulerTest, SplitsLargePutIntoChunks) {
    Scheduler::Config config;
    config.chunk_bytes = 16;
    config.deficit_quantum_bytes = 16;
    Scheduler scheduler(config);

    Clock::time_point now = Clock::time_point{} + std::chrono::seconds(1);
    EXPECT_EQ(scheduler.Enqueue(MakeRequest(
                  "tenant-a", Scheduler::OperationType::kPut, 40, now, "k")),
              3);

    std::optional<Request> first = scheduler.ScheduleOne(now);
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->tenant_id, "tenant-a");
    EXPECT_EQ(first->bytes, 16);
    EXPECT_EQ(first->total_bytes, 40);
    EXPECT_EQ(first->chunk_offset, 0);
    EXPECT_EQ(first->chunk_index, 0);
    EXPECT_EQ(first->chunk_count, 3);

    std::optional<Request> second = scheduler.ScheduleOne(now);
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->bytes, 16);
    EXPECT_EQ(second->chunk_offset, 16);
    EXPECT_EQ(second->chunk_index, 1);
    EXPECT_EQ(second->request_id, first->request_id);

    std::optional<Request> third = scheduler.ScheduleOne(now);
    ASSERT_TRUE(third.has_value());
    EXPECT_EQ(third->bytes, 8);
    EXPECT_EQ(third->chunk_offset, 32);
    EXPECT_EQ(third->chunk_index, 2);
    EXPECT_TRUE(scheduler.empty());
}

TEST(TenantQosSchedulerTest, PrioritizesGetOverQueuedPutChunks) {
    Scheduler::Config config;
    config.chunk_bytes = 16;
    config.deficit_quantum_bytes = 16;
    Scheduler scheduler(config);

    Clock::time_point now = Clock::time_point{} + std::chrono::seconds(1);
    scheduler.Enqueue(MakeRequest("tenant-a", Scheduler::OperationType::kPut,
                                  64, now, "large-put"));
    scheduler.Enqueue(MakeRequest("tenant-b", Scheduler::OperationType::kGet, 4,
                                  now, "small-get"));

    std::optional<Request> first = scheduler.ScheduleOne(now);
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->tenant_id, "tenant-b");
    EXPECT_EQ(first->operation_type, Scheduler::OperationType::kGet);
    EXPECT_EQ(first->priority_class, Scheduler::PriorityClass::kHigh);
}

TEST(TenantQosSchedulerTest, WeightedDeficitSchedulingHonorsTenantShare) {
    Scheduler::Config config;
    config.chunk_bytes = 100;
    config.deficit_quantum_bytes = 100;
    Scheduler scheduler(config);
    Scheduler::TenantConfig gold_config;
    gold_config.weight = 2;
    scheduler.SetTenantConfig("gold", gold_config);
    Scheduler::TenantConfig silver_config;
    silver_config.weight = 1;
    scheduler.SetTenantConfig("silver", silver_config);

    Clock::time_point now = Clock::time_point{} + std::chrono::seconds(1);
    for (int i = 0; i < 6; ++i) {
        scheduler.Enqueue(MakeRequest("gold", Scheduler::OperationType::kPut,
                                      100, now, "gold"));
        scheduler.Enqueue(MakeRequest("silver", Scheduler::OperationType::kPut,
                                      100, now, "silver"));
    }

    std::vector<std::string> order;
    for (int i = 0; i < 6; ++i) {
        std::optional<Request> request = scheduler.ScheduleOne(now);
        ASSERT_TRUE(request.has_value());
        order.push_back(request->tenant_id);
    }

    EXPECT_EQ(order, (std::vector<std::string>{"gold", "gold", "silver", "gold",
                                               "gold", "silver"}));
}

TEST(TenantQosSchedulerTest, TokenBucketDelaysTenantUntilRefill) {
    Scheduler::Config config;
    config.chunk_bytes = 100;
    config.deficit_quantum_bytes = 100;
    Scheduler scheduler(config);
    Scheduler::TenantConfig tenant_config;
    tenant_config.weight = 1;
    tenant_config.refill_bytes_per_ms = 1;
    tenant_config.bucket_bytes = 100;
    scheduler.SetTenantConfig("tenant-a", tenant_config);

    Clock::time_point now = Clock::time_point{} + std::chrono::seconds(1);
    scheduler.Enqueue(MakeRequest("tenant-a", Scheduler::OperationType::kPut,
                                  80, now, "first"));
    scheduler.Enqueue(MakeRequest("tenant-a", Scheduler::OperationType::kPut,
                                  80, now, "second"));

    std::optional<Request> first = scheduler.ScheduleOne(now);
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->object_key, "first");

    EXPECT_FALSE(scheduler.ScheduleOne(now).has_value());
    EXPECT_FALSE(
        scheduler.ScheduleOne(now + std::chrono::milliseconds(59)).has_value());

    std::optional<Request> second =
        scheduler.ScheduleOne(now + std::chrono::milliseconds(60));
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->object_key, "second");
}

TEST(TenantQosSchedulerTest, StarvationGuardAllowsOldBackgroundWork) {
    Scheduler::Config config;
    config.chunk_bytes = 100;
    config.deficit_quantum_bytes = 100;
    config.starvation_age = std::chrono::milliseconds(10);
    Scheduler scheduler(config);

    Clock::time_point start = Clock::time_point{} + std::chrono::seconds(1);
    scheduler.Enqueue(MakeRequest("bulk", Scheduler::OperationType::kBackground,
                                  10, start, "old-background"));
    scheduler.Enqueue(MakeRequest("latency", Scheduler::OperationType::kGet, 10,
                                  start + std::chrono::milliseconds(1),
                                  "new-get"));

    std::optional<Request> request =
        scheduler.ScheduleOne(start + std::chrono::milliseconds(11));
    ASSERT_TRUE(request.has_value());
    EXPECT_EQ(request->tenant_id, "bulk");
    EXPECT_EQ(request->object_key, "old-background");
    EXPECT_EQ(request->priority_class, Scheduler::PriorityClass::kBackground);
}

}  // namespace mooncake
