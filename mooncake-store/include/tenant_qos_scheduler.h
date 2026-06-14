#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace mooncake {

class TenantQosScheduler {
   public:
    using Clock = std::chrono::steady_clock;

    static constexpr uint64_t kDefaultChunkBytes = 16ULL * 1024ULL * 1024ULL;
    static constexpr const char* kDefaultTenantId = "default";

    enum class OperationType {
        kGet,
        kPut,
        kCopy,
        kBackground,
    };

    enum class PriorityClass {
        kUnspecified,
        kHigh,
        kNormal,
        kBackground,
    };

    struct TenantConfig {
        uint32_t weight = 1;
        uint64_t refill_bytes_per_ms = 0;
        uint64_t bucket_bytes = 0;
    };

    struct Config {
        uint64_t chunk_bytes = kDefaultChunkBytes;
        uint64_t deficit_quantum_bytes = 0;
        TenantConfig default_tenant_config;
        std::chrono::milliseconds starvation_age{0};
    };

    struct Request {
        std::string tenant_id = kDefaultTenantId;
        OperationType operation_type = OperationType::kPut;
        std::string object_key;
        uint64_t bytes = 0;
        Clock::time_point enqueue_time = Clock::time_point{};
        PriorityClass priority_class = PriorityClass::kUnspecified;

        uint64_t request_id = 0;
        uint64_t total_bytes = 0;
        uint64_t chunk_offset = 0;
        size_t chunk_index = 0;
        size_t chunk_count = 1;
    };

    TenantQosScheduler();
    explicit TenantQosScheduler(Config config);

    void SetTenantConfig(const std::string& tenant_id,
                         TenantConfig tenant_config);

    uint64_t Enqueue(Request request);

    std::optional<Request> ScheduleOne(Clock::time_point now);

    bool empty() const { return pending_requests_ == 0; }
    size_t pending() const { return pending_requests_; }

    static PriorityClass DefaultPriorityForOperation(OperationType op);

   private:
    static constexpr size_t kPriorityQueueCount = 3;

    struct TenantState {
        TenantConfig config;
        uint64_t deficit_bytes = 0;
        uint64_t tokens = 0;
        Clock::time_point last_refill_time = Clock::time_point{};
        std::array<std::deque<Request>, kPriorityQueueCount> queues;
    };

    Config config_;
    uint64_t next_request_id_ = 1;
    size_t pending_requests_ = 0;
    size_t rr_index_ = 0;
    std::vector<std::string> tenant_order_;
    std::unordered_map<std::string, TenantState> tenants_;
    std::unordered_map<std::string, TenantConfig> tenant_configs_;

    TenantState& GetOrCreateTenant(const std::string& tenant_id,
                                   Clock::time_point now);
    void EnqueueChunk(Request request);
    std::optional<size_t> PickTenant(PriorityClass priority,
                                     Clock::time_point now);
    const Request* PeekRequest(const TenantState& tenant,
                               PriorityClass priority,
                               Clock::time_point now) const;
    Request PopRequest(TenantState& tenant, PriorityClass priority,
                       Clock::time_point now);
    void RefillTokens(TenantState& tenant, Clock::time_point now);
    bool CanSpendTokens(const TenantState& tenant,
                        const Request& request) const;
    void SpendTokens(TenantState& tenant, const Request& request);
    uint64_t SchedulingCost(const Request& request) const;
    uint64_t DeficitQuantum(const TenantState& tenant) const;
    bool ShouldKeepTenantTurn(const TenantState& tenant, PriorityClass priority,
                              Clock::time_point now) const;
    std::deque<Request>& QueueForPriority(TenantState& tenant,
                                          PriorityClass priority);
    const std::deque<Request>& QueueForPriority(const TenantState& tenant,
                                                PriorityClass priority) const;
    static size_t QueueIndex(PriorityClass priority);
    static TenantConfig NormalizeTenantConfig(TenantConfig tenant_config);
};

}  // namespace mooncake
