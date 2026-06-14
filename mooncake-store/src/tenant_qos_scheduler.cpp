#include "tenant_qos_scheduler.h"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

namespace mooncake {

namespace {

uint64_t SaturatingAdd(uint64_t lhs, uint64_t rhs) {
    if (std::numeric_limits<uint64_t>::max() - lhs < rhs) {
        return std::numeric_limits<uint64_t>::max();
    }
    return lhs + rhs;
}

uint64_t SaturatingMultiply(uint64_t lhs, uint64_t rhs) {
    if (lhs != 0 && rhs > std::numeric_limits<uint64_t>::max() / lhs) {
        return std::numeric_limits<uint64_t>::max();
    }
    return lhs * rhs;
}

}  // namespace

TenantQosScheduler::TenantQosScheduler() : TenantQosScheduler(Config()) {}

TenantQosScheduler::TenantQosScheduler(Config config)
    : config_(std::move(config)) {
    config_.chunk_bytes = std::max<uint64_t>(1, config_.chunk_bytes);
    if (config_.deficit_quantum_bytes == 0) {
        config_.deficit_quantum_bytes = config_.chunk_bytes;
    }
    config_.default_tenant_config =
        NormalizeTenantConfig(config_.default_tenant_config);
}

void TenantQosScheduler::SetTenantConfig(const std::string& tenant_id,
                                         TenantConfig tenant_config) {
    std::string normalized_tenant_id =
        tenant_id.empty() ? kDefaultTenantId : tenant_id;
    TenantConfig normalized_config = NormalizeTenantConfig(tenant_config);
    tenant_configs_[normalized_tenant_id] = normalized_config;

    auto tenant_it = tenants_.find(normalized_tenant_id);
    if (tenant_it == tenants_.end()) {
        return;
    }

    bool was_token_bucket_disabled =
        tenant_it->second.config.bucket_bytes == 0 ||
        tenant_it->second.config.refill_bytes_per_ms == 0;
    tenant_it->second.config = normalized_config;
    if (normalized_config.bucket_bytes == 0 ||
        normalized_config.refill_bytes_per_ms == 0) {
        tenant_it->second.tokens = 0;
    } else if (was_token_bucket_disabled) {
        tenant_it->second.tokens = normalized_config.bucket_bytes;
    } else {
        tenant_it->second.tokens =
            std::min(tenant_it->second.tokens, normalized_config.bucket_bytes);
    }
}

uint64_t TenantQosScheduler::Enqueue(Request request) {
    if (request.tenant_id.empty()) {
        request.tenant_id = kDefaultTenantId;
    }
    if (request.enqueue_time == Clock::time_point{}) {
        request.enqueue_time = Clock::now();
    }
    if (request.priority_class == PriorityClass::kUnspecified) {
        request.priority_class =
            DefaultPriorityForOperation(request.operation_type);
    }
    if (request.total_bytes == 0) {
        request.total_bytes = request.bytes;
    }
    if (request.request_id == 0) {
        request.request_id = next_request_id_++;
    }

    const bool should_chunk = request.operation_type == OperationType::kPut &&
                              request.bytes > config_.chunk_bytes;
    if (!should_chunk) {
        request.chunk_count = 1;
        request.chunk_index = 0;
        request.chunk_offset = 0;
        EnqueueChunk(std::move(request));
        return 1;
    }

    const uint64_t original_bytes = request.bytes;
    const uint64_t chunk_count =
        (original_bytes + config_.chunk_bytes - 1) / config_.chunk_bytes;
    for (uint64_t offset = 0, chunk_index = 0; offset < original_bytes;
         offset += config_.chunk_bytes, ++chunk_index) {
        Request chunk = request;
        chunk.bytes = std::min(config_.chunk_bytes, original_bytes - offset);
        chunk.total_bytes = original_bytes;
        chunk.chunk_offset = offset;
        chunk.chunk_index = static_cast<size_t>(chunk_index);
        chunk.chunk_count = static_cast<size_t>(chunk_count);
        EnqueueChunk(std::move(chunk));
    }
    return chunk_count;
}

std::optional<TenantQosScheduler::Request> TenantQosScheduler::ScheduleOne(
    Clock::time_point now) {
    static constexpr PriorityClass kPriorities[] = {
        PriorityClass::kHigh,
        PriorityClass::kNormal,
        PriorityClass::kBackground,
    };

    for (PriorityClass priority : kPriorities) {
        std::optional<size_t> tenant_index = PickTenant(priority, now);
        if (!tenant_index.has_value()) {
            continue;
        }

        TenantState& tenant = tenants_.at(tenant_order_[*tenant_index]);
        Request request = PopRequest(tenant, priority, now);
        const uint64_t cost = SchedulingCost(request);
        if (tenant.deficit_bytes >= cost) {
            tenant.deficit_bytes -= cost;
        } else {
            tenant.deficit_bytes = 0;
        }
        SpendTokens(tenant, request);
        --pending_requests_;

        if (!ShouldKeepTenantTurn(tenant, priority, now) ||
            tenant_order_.empty()) {
            rr_index_ = tenant_order_.empty()
                            ? 0
                            : ((*tenant_index + 1) % tenant_order_.size());
        } else {
            rr_index_ = *tenant_index;
        }
        return request;
    }

    return std::nullopt;
}

TenantQosScheduler::PriorityClass
TenantQosScheduler::DefaultPriorityForOperation(OperationType op) {
    switch (op) {
        case OperationType::kGet:
            return PriorityClass::kHigh;
        case OperationType::kPut:
            return PriorityClass::kNormal;
        case OperationType::kCopy:
        case OperationType::kBackground:
            return PriorityClass::kBackground;
    }
    return PriorityClass::kNormal;
}

TenantQosScheduler::TenantState& TenantQosScheduler::GetOrCreateTenant(
    const std::string& tenant_id, Clock::time_point now) {
    auto tenant_it = tenants_.find(tenant_id);
    if (tenant_it != tenants_.end()) {
        return tenant_it->second;
    }

    TenantConfig tenant_config = config_.default_tenant_config;
    auto config_it = tenant_configs_.find(tenant_id);
    if (config_it != tenant_configs_.end()) {
        tenant_config = config_it->second;
    }

    TenantState tenant;
    tenant.config = tenant_config;
    tenant.last_refill_time = now;
    if (tenant.config.bucket_bytes != 0 &&
        tenant.config.refill_bytes_per_ms != 0) {
        tenant.tokens = tenant.config.bucket_bytes;
    }

    tenant_order_.push_back(tenant_id);
    auto [inserted_it, _] = tenants_.emplace(tenant_id, std::move(tenant));
    return inserted_it->second;
}

void TenantQosScheduler::EnqueueChunk(Request request) {
    TenantState& tenant =
        GetOrCreateTenant(request.tenant_id, request.enqueue_time);
    QueueForPriority(tenant, request.priority_class)
        .push_back(std::move(request));
    ++pending_requests_;
}

std::optional<size_t> TenantQosScheduler::PickTenant(PriorityClass priority,
                                                     Clock::time_point now) {
    if (tenant_order_.empty()) {
        return std::nullopt;
    }

    for (size_t scanned = 0; scanned < tenant_order_.size(); ++scanned) {
        size_t tenant_index = (rr_index_ + scanned) % tenant_order_.size();
        TenantState& tenant = tenants_.at(tenant_order_[tenant_index]);
        RefillTokens(tenant, now);

        const Request* request = PeekRequest(tenant, priority, now);
        if (request == nullptr || !CanSpendTokens(tenant, *request)) {
            continue;
        }

        const uint64_t cost = SchedulingCost(*request);
        if (tenant.deficit_bytes < cost) {
            tenant.deficit_bytes =
                SaturatingAdd(tenant.deficit_bytes, DeficitQuantum(tenant));
        }
        if (tenant.deficit_bytes >= cost) {
            return tenant_index;
        }
    }

    return std::nullopt;
}

const TenantQosScheduler::Request* TenantQosScheduler::PeekRequest(
    const TenantState& tenant, PriorityClass priority,
    Clock::time_point now) const {
    const auto& priority_queue = QueueForPriority(tenant, priority);
    if (!priority_queue.empty()) {
        return &priority_queue.front();
    }

    if (priority != PriorityClass::kHigh ||
        config_.starvation_age.count() <= 0) {
        return nullptr;
    }

    const Request* oldest = nullptr;
    const auto& normal_queue = QueueForPriority(tenant, PriorityClass::kNormal);
    const auto& background_queue =
        QueueForPriority(tenant, PriorityClass::kBackground);
    if (!normal_queue.empty() &&
        now - normal_queue.front().enqueue_time >= config_.starvation_age) {
        oldest = &normal_queue.front();
    }
    if (!background_queue.empty() &&
        now - background_queue.front().enqueue_time >= config_.starvation_age &&
        (oldest == nullptr ||
         background_queue.front().enqueue_time < oldest->enqueue_time)) {
        oldest = &background_queue.front();
    }
    return oldest;
}

TenantQosScheduler::Request TenantQosScheduler::PopRequest(
    TenantState& tenant, PriorityClass priority, Clock::time_point now) {
    std::deque<Request>* queue = &QueueForPriority(tenant, priority);
    if (queue->empty() && priority == PriorityClass::kHigh &&
        config_.starvation_age.count() > 0) {
        auto& normal_queue = QueueForPriority(tenant, PriorityClass::kNormal);
        auto& background_queue =
            QueueForPriority(tenant, PriorityClass::kBackground);

        const bool normal_is_starved =
            !normal_queue.empty() &&
            now - normal_queue.front().enqueue_time >= config_.starvation_age;
        const bool background_is_starved =
            !background_queue.empty() &&
            now - background_queue.front().enqueue_time >=
                config_.starvation_age;

        if (normal_is_starved && background_is_starved) {
            queue = normal_queue.front().enqueue_time <=
                            background_queue.front().enqueue_time
                        ? &normal_queue
                        : &background_queue;
        } else if (normal_is_starved) {
            queue = &normal_queue;
        } else if (background_is_starved) {
            queue = &background_queue;
        }
    }

    Request request = std::move(queue->front());
    queue->pop_front();
    return request;
}

void TenantQosScheduler::RefillTokens(TenantState& tenant,
                                      Clock::time_point now) {
    if (tenant.config.bucket_bytes == 0 ||
        tenant.config.refill_bytes_per_ms == 0) {
        tenant.last_refill_time = now;
        return;
    }
    if (now <= tenant.last_refill_time) {
        return;
    }

    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          now - tenant.last_refill_time)
                          .count();
    if (elapsed_ms <= 0) {
        return;
    }

    uint64_t refill_bytes = SaturatingMultiply(
        tenant.config.refill_bytes_per_ms, static_cast<uint64_t>(elapsed_ms));
    tenant.tokens = std::min(tenant.config.bucket_bytes,
                             SaturatingAdd(tenant.tokens, refill_bytes));
    tenant.last_refill_time += std::chrono::milliseconds(elapsed_ms);
}

bool TenantQosScheduler::CanSpendTokens(const TenantState& tenant,
                                        const Request& request) const {
    if (tenant.config.bucket_bytes == 0 ||
        tenant.config.refill_bytes_per_ms == 0) {
        return true;
    }
    return tenant.tokens >=
           std::min(SchedulingCost(request), tenant.config.bucket_bytes);
}

void TenantQosScheduler::SpendTokens(TenantState& tenant,
                                     const Request& request) {
    if (tenant.config.bucket_bytes == 0 ||
        tenant.config.refill_bytes_per_ms == 0) {
        return;
    }
    uint64_t cost =
        std::min(SchedulingCost(request), tenant.config.bucket_bytes);
    tenant.tokens = tenant.tokens >= cost ? tenant.tokens - cost : 0;
}

uint64_t TenantQosScheduler::SchedulingCost(const Request& request) const {
    return request.bytes;
}

uint64_t TenantQosScheduler::DeficitQuantum(const TenantState& tenant) const {
    return SaturatingMultiply(config_.deficit_quantum_bytes,
                              tenant.config.weight);
}

bool TenantQosScheduler::ShouldKeepTenantTurn(const TenantState& tenant,
                                              PriorityClass priority,
                                              Clock::time_point now) const {
    const Request* next_request = PeekRequest(tenant, priority, now);
    if (next_request == nullptr || !CanSpendTokens(tenant, *next_request)) {
        return false;
    }
    return tenant.deficit_bytes >= SchedulingCost(*next_request);
}

std::deque<TenantQosScheduler::Request>& TenantQosScheduler::QueueForPriority(
    TenantState& tenant, PriorityClass priority) {
    return tenant.queues[QueueIndex(priority)];
}

const std::deque<TenantQosScheduler::Request>&
TenantQosScheduler::QueueForPriority(const TenantState& tenant,
                                     PriorityClass priority) const {
    return tenant.queues[QueueIndex(priority)];
}

size_t TenantQosScheduler::QueueIndex(PriorityClass priority) {
    switch (priority) {
        case PriorityClass::kHigh:
            return 0;
        case PriorityClass::kNormal:
            return 1;
        case PriorityClass::kBackground:
            return 2;
        case PriorityClass::kUnspecified:
            break;
    }
    throw std::invalid_argument("unsupported TenantQosScheduler priority");
}

TenantQosScheduler::TenantConfig TenantQosScheduler::NormalizeTenantConfig(
    TenantConfig tenant_config) {
    tenant_config.weight = std::max<uint32_t>(1, tenant_config.weight);
    return tenant_config;
}

}  // namespace mooncake
