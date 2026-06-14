#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "allocator.h"
#include "client_service.h"
#include "master_config.h"
#include "test_server_helpers.h"
#include "types.h"
#include "utils.h"

DEFINE_string(scenario, "unloaded_get",
              "Benchmark scenario: unloaded_get | single_tenant_interference | "
              "two_tenant_interference");
DEFINE_string(output_dir, "qos_results", "Directory for CSV/JSON output");
DEFINE_string(protocol, "tcp", "Transfer protocol: tcp|rdma");
DEFINE_string(device_name, "", "RDMA device name, only used for rdma");
DEFINE_bool(start_inproc_master, false,
            "Start an embedded in-process master instead of connecting to "
            "--master_address");
DEFINE_string(master_address, "",
              "Master RPC address. Required unless --start_inproc_master=true");
DEFINE_double(duration_sec, 5.0, "Foreground Get measurement duration");
DEFINE_double(warmup_sec, 0.5,
              "Background Put warmup before foreground Get starts");
DEFINE_uint64(small_get_bytes, 1024 * 1024,
              "Size of the latency-sensitive Get object");
DEFINE_uint64(large_put_bytes, 64ULL * 1024ULL * 1024ULL,
              "Size of each background Put object");
DEFINE_uint64(segment_bytes, 2ULL * 1024ULL * 1024ULL * 1024ULL,
              "Provider memory segment size");
DEFINE_int32(get_threads, 1, "Foreground Get worker count");
DEFINE_int32(put_threads, 1, "Background Put worker count");
DEFINE_int32(put_batch_size, 1,
             "Background Put batch size. 1 uses Client::Put; >1 uses "
             "Client::BatchPut");
DEFINE_double(get_slo_ms, 10.0, "Get SLO threshold in milliseconds");
DEFINE_bool(remove_bulk_keys, true,
            "Remove background bulk keys after each successful Put/BatchPut");
DEFINE_string(tenant_a, "tenant-a", "Bulk background tenant");
DEFINE_string(tenant_b, "tenant-b", "Latency-sensitive foreground tenant");
DEFINE_string(single_tenant, "default",
              "Tenant used for single_tenant_interference");
DEFINE_int32(preload_get_objects, 1,
             "Number of small objects preloaded for foreground Get");

namespace mooncake {
namespace benchmark {
namespace {

using Clock = std::chrono::steady_clock;

constexpr double kMicrosPerMilli = 1000.0;
constexpr double kBytesPerMiB = 1024.0 * 1024.0;

struct ScenarioConfig {
    bool enable_background_put = false;
    std::string put_tenant_id;
    std::string get_tenant_id;
    std::string output_stem;
};

struct ClientContext {
    std::shared_ptr<Client> client;
    void* local_buffer = nullptr;
    size_t local_buffer_size = 0;

    ~ClientContext() {
        if (client && local_buffer) {
            (void)client->unregisterLocalMemory(local_buffer,
                                                /*update_metadata=*/false);
        }
        if (local_buffer) {
            std::free(local_buffer);
        }
    }
};

struct ProviderContext {
    std::shared_ptr<Client> client;
    void* segment = nullptr;
    size_t segment_size = 0;

    ~ProviderContext() {
        if (client && segment) {
            (void)client->UnmountSegment(segment, segment_size);
        }
        if (segment) {
            std::free(segment);
        }
    }
};

struct WorkerStats {
    uint64_t get_success = 0;
    uint64_t get_fail = 0;
    uint64_t get_bytes = 0;
    uint64_t get_slo_violations = 0;
    uint64_t put_success = 0;
    uint64_t put_fail = 0;
    uint64_t put_bytes = 0;
    std::vector<double> get_latency_us;
};

struct Summary {
    std::string scenario;
    std::string put_tenant_id;
    std::string get_tenant_id;
    std::string protocol;
    uint64_t small_get_bytes = 0;
    uint64_t large_put_bytes = 0;
    uint64_t segment_bytes = 0;
    int get_threads = 0;
    int put_threads = 0;
    int put_batch_size = 0;
    double configured_duration_sec = 0;
    double measured_duration_sec = 0;
    double warmup_sec = 0;
    double get_slo_ms = 0;
    uint64_t get_success = 0;
    uint64_t get_fail = 0;
    uint64_t get_slo_violations = 0;
    uint64_t put_success = 0;
    uint64_t put_fail = 0;
    uint64_t get_bytes = 0;
    uint64_t put_bytes = 0;
    double get_p50_us = 0;
    double get_p95_us = 0;
    double get_p99_us = 0;
    double get_p999_us = 0;
    double get_slo_violation_rate = 0;
    double get_failure_rate = 0;
    double put_mib_per_sec = 0;
    double get_mib_per_sec = 0;
    double aggregate_mib_per_sec = 0;
    double tenant_a_mib_per_sec = 0;
    double tenant_b_mib_per_sec = 0;
    double tenant_a_bandwidth_share = 0;
    double tenant_b_bandwidth_share = 0;
};

std::optional<ScenarioConfig> BuildScenarioConfig() {
    if (FLAGS_scenario == "unloaded_get") {
        return ScenarioConfig{/*enable_background_put=*/false, "",
                              FLAGS_tenant_b, "baseline_get_only"};
    }
    if (FLAGS_scenario == "single_tenant_interference") {
        return ScenarioConfig{/*enable_background_put=*/true,
                              FLAGS_single_tenant, FLAGS_single_tenant,
                              "baseline_single_tenant"};
    }
    if (FLAGS_scenario == "two_tenant_interference") {
        return ScenarioConfig{/*enable_background_put=*/true, FLAGS_tenant_a,
                              FLAGS_tenant_b, "baseline_two_tenant"};
    }
    return std::nullopt;
}

size_t LocalBufferAllocationBytes(size_t requested_bytes) {
    const size_t alignment = facebook::cachelib::Slab::kSize;
    return align_up(std::max(requested_bytes, alignment), alignment);
}

std::optional<std::shared_ptr<Client>> CreateClient(
    const std::string& hostname, const std::string& master_address,
    const std::string& tenant_id) {
    std::optional<std::string> device_names = std::nullopt;
    if (!FLAGS_device_name.empty()) {
        device_names = FLAGS_device_name;
    }
    return Client::Create(hostname, "P2PHANDSHAKE", FLAGS_protocol,
                          device_names, master_address,
                          /*transfer_engine=*/nullptr,
                          /*labels=*/{}, tenant_id);
}

std::optional<std::unique_ptr<ClientContext>> CreateWorkerClient(
    const std::string& hostname, const std::string& master_address,
    const std::string& tenant_id, size_t local_buffer_size) {
    auto client_opt = CreateClient(hostname, master_address, tenant_id);
    if (!client_opt.has_value()) {
        LOG(ERROR) << "Failed to create client hostname=" << hostname
                   << " tenant_id=" << tenant_id;
        return std::nullopt;
    }

    auto ctx = std::make_unique<ClientContext>();
    ctx->client = client_opt.value();
    ctx->local_buffer_size = LocalBufferAllocationBytes(local_buffer_size);
    ctx->local_buffer = allocate_buffer_allocator_memory(ctx->local_buffer_size,
                                                         FLAGS_protocol);
    if (ctx->local_buffer == nullptr) {
        LOG(ERROR) << "Failed to allocate local buffer size="
                   << ctx->local_buffer_size;
        return std::nullopt;
    }

    auto reg = ctx->client->RegisterLocalMemory(
        ctx->local_buffer, ctx->local_buffer_size, "cpu:0",
        /*remote_accessible=*/false, /*update_metadata=*/false);
    if (!reg.has_value()) {
        LOG(ERROR) << "Failed to register local memory error="
                   << toString(reg.error());
        return std::nullopt;
    }

    return std::optional<std::unique_ptr<ClientContext>>(std::move(ctx));
}

std::vector<Slice> MakeSlices(void* base, size_t bytes) {
    std::vector<Slice> slices;
    char* cursor = static_cast<char*>(base);
    size_t remaining = bytes;
    while (remaining > 0) {
        size_t chunk = std::min<size_t>(remaining, kMaxSliceSize);
        slices.push_back(Slice{cursor, chunk});
        cursor += chunk;
        remaining -= chunk;
    }
    return slices;
}

void FillPattern(void* base, size_t bytes, uint8_t seed) {
    auto* cursor = static_cast<uint8_t*>(base);
    for (size_t i = 0; i < bytes; ++i) {
        cursor[i] = static_cast<uint8_t>(seed + i);
    }
}

double Percentile(std::vector<double> sorted_values, double percentile) {
    if (sorted_values.empty()) {
        return 0.0;
    }
    std::sort(sorted_values.begin(), sorted_values.end());
    const double rank =
        std::ceil((percentile / 100.0) * sorted_values.size()) - 1.0;
    size_t index = static_cast<size_t>(
        std::clamp(rank, 0.0, static_cast<double>(sorted_values.size() - 1)));
    return sorted_values[index];
}

std::string CsvEscape(const std::string& value) {
    if (value.find_first_of(",\"\n\r") == std::string::npos) {
        return value;
    }
    std::string escaped = "\"";
    for (char c : value) {
        if (c == '"') {
            escaped += "\"\"";
        } else {
            escaped += c;
        }
    }
    escaped += "\"";
    return escaped;
}

std::string JsonEscape(const std::string& value) {
    std::string escaped;
    escaped.reserve(value.size() + 8);
    for (char c : value) {
        switch (c) {
            case '\\':
                escaped += "\\\\";
                break;
            case '"':
                escaped += "\\\"";
                break;
            case '\n':
                escaped += "\\n";
                break;
            case '\r':
                escaped += "\\r";
                break;
            case '\t':
                escaped += "\\t";
                break;
            default:
                escaped += c;
        }
    }
    return escaped;
}

void PutWorker(std::unique_ptr<ClientContext> ctx, int worker_id,
               const ScenarioConfig& scenario, std::atomic<bool>& stop,
               WorkerStats& stats) {
    FillPattern(ctx->local_buffer, FLAGS_large_put_bytes,
                static_cast<uint8_t>(worker_id + 17));
    std::vector<Slice> put_slices =
        MakeSlices(ctx->local_buffer, FLAGS_large_put_bytes);
    ReplicateConfig config;
    config.replica_num = 1;

    uint64_t seq = 0;
    while (!stop.load(std::memory_order_relaxed)) {
        std::vector<std::string> keys;
        keys.reserve(std::max(1, FLAGS_put_batch_size));

        bool success = true;
        if (FLAGS_put_batch_size <= 1) {
            std::string key = scenario.output_stem + "-bulk-" +
                              std::to_string(worker_id) + "-" +
                              std::to_string(seq++);
            auto result = ctx->client->Put(key, put_slices, config);
            success = result.has_value();
            keys.push_back(std::move(key));
        } else {
            std::vector<std::vector<Slice>> batched_slices;
            batched_slices.reserve(FLAGS_put_batch_size);
            for (int i = 0; i < FLAGS_put_batch_size; ++i) {
                keys.push_back(scenario.output_stem + "-bulk-" +
                               std::to_string(worker_id) + "-" +
                               std::to_string(seq++));
                batched_slices.push_back(put_slices);
            }
            auto results = ctx->client->BatchPut(keys, batched_slices, config);
            success = results.size() == keys.size();
            for (const auto& result : results) {
                success = success && result.has_value();
            }
        }

        if (success) {
            stats.put_success += keys.size();
            stats.put_bytes += FLAGS_large_put_bytes * keys.size();
            if (FLAGS_remove_bulk_keys) {
                for (const auto& key : keys) {
                    (void)ctx->client->Remove(key, /*force=*/true);
                }
            }
        } else {
            stats.put_fail += keys.size();
        }
    }
}

void GetWorker(std::unique_ptr<ClientContext> ctx,
               const std::vector<std::string>& keys, std::atomic<bool>& start,
               std::atomic<bool>& stop, WorkerStats& stats) {
    std::vector<Slice> get_slices =
        MakeSlices(ctx->local_buffer, FLAGS_small_get_bytes);
    while (!start.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    size_t index = 0;
    const double slo_us = FLAGS_get_slo_ms * kMicrosPerMilli;
    while (!stop.load(std::memory_order_relaxed)) {
        const std::string& key = keys[index++ % keys.size()];
        auto begin = Clock::now();
        auto result = ctx->client->Get(key, get_slices);
        auto end = Clock::now();
        double latency_us =
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin)
                .count() /
            1000.0;

        if (result.has_value()) {
            stats.get_success++;
            stats.get_bytes += FLAGS_small_get_bytes;
            stats.get_latency_us.push_back(latency_us);
            if (latency_us > slo_us) {
                stats.get_slo_violations++;
            }
        } else {
            stats.get_fail++;
        }
    }
}

Summary BuildSummary(const ScenarioConfig& scenario,
                     const std::vector<WorkerStats>& get_stats,
                     const std::vector<WorkerStats>& put_stats,
                     double measured_duration_sec) {
    Summary summary;
    summary.scenario = FLAGS_scenario;
    summary.put_tenant_id = scenario.put_tenant_id;
    summary.get_tenant_id = scenario.get_tenant_id;
    summary.protocol = FLAGS_protocol;
    summary.small_get_bytes = FLAGS_small_get_bytes;
    summary.large_put_bytes = FLAGS_large_put_bytes;
    summary.segment_bytes = FLAGS_segment_bytes;
    summary.get_threads = FLAGS_get_threads;
    summary.put_threads =
        scenario.enable_background_put ? FLAGS_put_threads : 0;
    summary.put_batch_size = FLAGS_put_batch_size;
    summary.configured_duration_sec = FLAGS_duration_sec;
    summary.measured_duration_sec = measured_duration_sec;
    summary.warmup_sec = scenario.enable_background_put ? FLAGS_warmup_sec : 0;
    summary.get_slo_ms = FLAGS_get_slo_ms;

    std::vector<double> get_latencies;
    for (const auto& stats : get_stats) {
        summary.get_success += stats.get_success;
        summary.get_fail += stats.get_fail;
        summary.get_bytes += stats.get_bytes;
        summary.get_slo_violations += stats.get_slo_violations;
        get_latencies.insert(get_latencies.end(), stats.get_latency_us.begin(),
                             stats.get_latency_us.end());
    }
    for (const auto& stats : put_stats) {
        summary.put_success += stats.put_success;
        summary.put_fail += stats.put_fail;
        summary.put_bytes += stats.put_bytes;
    }

    summary.get_p50_us = Percentile(get_latencies, 50.0);
    summary.get_p95_us = Percentile(get_latencies, 95.0);
    summary.get_p99_us = Percentile(get_latencies, 99.0);
    summary.get_p999_us = Percentile(get_latencies, 99.9);

    if (summary.get_success > 0) {
        summary.get_slo_violation_rate =
            static_cast<double>(summary.get_slo_violations) /
            static_cast<double>(summary.get_success);
    }
    const uint64_t total_get_attempts = summary.get_success + summary.get_fail;
    if (total_get_attempts > 0) {
        summary.get_failure_rate = static_cast<double>(summary.get_fail) /
                                   static_cast<double>(total_get_attempts);
    }
    if (measured_duration_sec > 0.0) {
        summary.put_mib_per_sec =
            summary.put_bytes / kBytesPerMiB / measured_duration_sec;
        summary.get_mib_per_sec =
            summary.get_bytes / kBytesPerMiB / measured_duration_sec;
        summary.aggregate_mib_per_sec =
            (summary.put_bytes + summary.get_bytes) / kBytesPerMiB /
            measured_duration_sec;
    }

    if (FLAGS_scenario == "two_tenant_interference") {
        summary.tenant_a_mib_per_sec = summary.put_mib_per_sec;
        summary.tenant_b_mib_per_sec = summary.get_mib_per_sec;
    } else if (FLAGS_scenario == "single_tenant_interference") {
        summary.tenant_b_mib_per_sec =
            summary.put_mib_per_sec + summary.get_mib_per_sec;
    } else {
        summary.tenant_b_mib_per_sec = summary.get_mib_per_sec;
    }

    const double tenant_total =
        summary.tenant_a_mib_per_sec + summary.tenant_b_mib_per_sec;
    if (tenant_total > 0.0) {
        summary.tenant_a_bandwidth_share =
            summary.tenant_a_mib_per_sec / tenant_total;
        summary.tenant_b_bandwidth_share =
            summary.tenant_b_mib_per_sec / tenant_total;
    }
    return summary;
}

void WriteCsv(const Summary& summary, const std::filesystem::path& path) {
    std::ofstream out(path);
    out << "scenario,put_tenant_id,get_tenant_id,protocol,small_get_bytes,"
           "large_put_bytes,segment_bytes,get_threads,put_threads,"
           "put_batch_size,configured_duration_sec,measured_duration_sec,"
           "warmup_sec,get_slo_ms,get_success,get_fail,"
           "get_slo_violations,get_p50_us,get_p95_us,get_p99_us,"
           "get_p999_us,get_slo_violation_rate,get_failure_rate,"
           "put_success,put_fail,put_mib_per_sec,get_mib_per_sec,"
           "aggregate_mib_per_sec,tenant_a_mib_per_sec,tenant_b_mib_per_sec,"
           "tenant_a_bandwidth_share,tenant_b_bandwidth_share\n";
    out << CsvEscape(summary.scenario) << ","
        << CsvEscape(summary.put_tenant_id) << ","
        << CsvEscape(summary.get_tenant_id) << ","
        << CsvEscape(summary.protocol) << "," << summary.small_get_bytes << ","
        << summary.large_put_bytes << "," << summary.segment_bytes << ","
        << summary.get_threads << "," << summary.put_threads << ","
        << summary.put_batch_size << "," << summary.configured_duration_sec
        << "," << summary.measured_duration_sec << "," << summary.warmup_sec
        << "," << summary.get_slo_ms << "," << summary.get_success << ","
        << summary.get_fail << "," << summary.get_slo_violations << ","
        << summary.get_p50_us << "," << summary.get_p95_us << ","
        << summary.get_p99_us << "," << summary.get_p999_us << ","
        << summary.get_slo_violation_rate << "," << summary.get_failure_rate
        << "," << summary.put_success << "," << summary.put_fail << ","
        << summary.put_mib_per_sec << "," << summary.get_mib_per_sec << ","
        << summary.aggregate_mib_per_sec << "," << summary.tenant_a_mib_per_sec
        << "," << summary.tenant_b_mib_per_sec << ","
        << summary.tenant_a_bandwidth_share << ","
        << summary.tenant_b_bandwidth_share << "\n";
}

void WriteJson(const Summary& summary, const std::filesystem::path& path) {
    std::ofstream out(path);
    out << std::setprecision(12);
    out << "{\n";
    out << "  \"scenario\": \"" << JsonEscape(summary.scenario) << "\",\n";
    out << "  \"put_tenant_id\": \"" << JsonEscape(summary.put_tenant_id)
        << "\",\n";
    out << "  \"get_tenant_id\": \"" << JsonEscape(summary.get_tenant_id)
        << "\",\n";
    out << "  \"protocol\": \"" << JsonEscape(summary.protocol) << "\",\n";
    out << "  \"small_get_bytes\": " << summary.small_get_bytes << ",\n";
    out << "  \"large_put_bytes\": " << summary.large_put_bytes << ",\n";
    out << "  \"segment_bytes\": " << summary.segment_bytes << ",\n";
    out << "  \"get_threads\": " << summary.get_threads << ",\n";
    out << "  \"put_threads\": " << summary.put_threads << ",\n";
    out << "  \"put_batch_size\": " << summary.put_batch_size << ",\n";
    out << "  \"configured_duration_sec\": " << summary.configured_duration_sec
        << ",\n";
    out << "  \"measured_duration_sec\": " << summary.measured_duration_sec
        << ",\n";
    out << "  \"warmup_sec\": " << summary.warmup_sec << ",\n";
    out << "  \"get_slo_ms\": " << summary.get_slo_ms << ",\n";
    out << "  \"get_success\": " << summary.get_success << ",\n";
    out << "  \"get_fail\": " << summary.get_fail << ",\n";
    out << "  \"get_slo_violations\": " << summary.get_slo_violations << ",\n";
    out << "  \"get_p50_us\": " << summary.get_p50_us << ",\n";
    out << "  \"get_p95_us\": " << summary.get_p95_us << ",\n";
    out << "  \"get_p99_us\": " << summary.get_p99_us << ",\n";
    out << "  \"get_p999_us\": " << summary.get_p999_us << ",\n";
    out << "  \"get_slo_violation_rate\": " << summary.get_slo_violation_rate
        << ",\n";
    out << "  \"get_failure_rate\": " << summary.get_failure_rate << ",\n";
    out << "  \"put_success\": " << summary.put_success << ",\n";
    out << "  \"put_fail\": " << summary.put_fail << ",\n";
    out << "  \"put_mib_per_sec\": " << summary.put_mib_per_sec << ",\n";
    out << "  \"get_mib_per_sec\": " << summary.get_mib_per_sec << ",\n";
    out << "  \"aggregate_mib_per_sec\": " << summary.aggregate_mib_per_sec
        << ",\n";
    out << "  \"tenant_a_mib_per_sec\": " << summary.tenant_a_mib_per_sec
        << ",\n";
    out << "  \"tenant_b_mib_per_sec\": " << summary.tenant_b_mib_per_sec
        << ",\n";
    out << "  \"tenant_a_bandwidth_share\": "
        << summary.tenant_a_bandwidth_share << ",\n";
    out << "  \"tenant_b_bandwidth_share\": "
        << summary.tenant_b_bandwidth_share << "\n";
    out << "}\n";
}

void LogSummary(const Summary& summary) {
    LOG(INFO) << "scenario=" << summary.scenario
              << " get_p50_us=" << summary.get_p50_us
              << " get_p95_us=" << summary.get_p95_us
              << " get_p99_us=" << summary.get_p99_us
              << " get_p999_us=" << summary.get_p999_us
              << " get_slo_violation_rate=" << summary.get_slo_violation_rate
              << " put_mib_per_sec=" << summary.put_mib_per_sec
              << " aggregate_mib_per_sec=" << summary.aggregate_mib_per_sec;
}

int Run() {
    auto scenario_opt = BuildScenarioConfig();
    if (!scenario_opt.has_value()) {
        LOG(ERROR) << "Unsupported scenario: " << FLAGS_scenario;
        return 2;
    }
    const ScenarioConfig scenario = scenario_opt.value();

    if (FLAGS_duration_sec <= 0.0 || FLAGS_warmup_sec < 0.0 ||
        FLAGS_small_get_bytes == 0 || FLAGS_large_put_bytes == 0 ||
        FLAGS_get_threads <= 0 || FLAGS_put_threads < 0 ||
        FLAGS_put_batch_size <= 0 || FLAGS_preload_get_objects <= 0) {
        LOG(ERROR) << "Invalid benchmark flags";
        return 2;
    }
    if (FLAGS_segment_bytes < FLAGS_large_put_bytes + FLAGS_small_get_bytes) {
        LOG(ERROR) << "segment_bytes is too small for configured objects";
        return 2;
    }

    std::unique_ptr<testing::InProcMaster> inproc_master;
    std::string master_address = FLAGS_master_address;
    if (FLAGS_start_inproc_master) {
        inproc_master = std::make_unique<testing::InProcMaster>();
        auto master_config = InProcMasterConfigBuilder()
                                 .set_http_metadata_port(0)
                                 .set_enable_offload(false)
                                 .set_enable_disk_eviction(false)
                                 .build();
        if (!inproc_master->Start(master_config)) {
            LOG(ERROR) << "Failed to start in-process master";
            return 1;
        }
        master_address = inproc_master->master_address();
    } else if (master_address.empty()) {
        LOG(ERROR) << "--master_address is required when "
                      "--start_inproc_master=false";
        return 2;
    }

    std::vector<int> client_ports = getFreeTcpPorts(
        2 + FLAGS_get_threads +
        (scenario.enable_background_put ? FLAGS_put_threads : 0));
    size_t port_index = 0;

    ProviderContext provider;
    auto provider_client =
        CreateClient("127.0.0.1:" + std::to_string(client_ports[port_index++]),
                     master_address, "provider");
    if (!provider_client.has_value()) {
        LOG(ERROR) << "Failed to create provider client";
        return 1;
    }
    provider.client = provider_client.value();
    provider.segment_size = LocalBufferAllocationBytes(FLAGS_segment_bytes);
    provider.segment =
        allocate_buffer_allocator_memory(provider.segment_size, FLAGS_protocol);
    if (provider.segment == nullptr) {
        LOG(ERROR) << "Failed to allocate provider segment";
        return 1;
    }
    auto mount = provider.client->MountSegment(
        provider.segment, provider.segment_size, FLAGS_protocol);
    if (!mount.has_value()) {
        LOG(ERROR) << "Failed to mount provider segment error="
                   << toString(mount.error());
        return 1;
    }

    auto preload_ctx = CreateWorkerClient(
        "127.0.0.1:" + std::to_string(client_ports[port_index++]),
        master_address, scenario.get_tenant_id, FLAGS_small_get_bytes);
    if (!preload_ctx.has_value()) {
        return 1;
    }
    FillPattern((*preload_ctx)->local_buffer, FLAGS_small_get_bytes, 3);
    std::vector<Slice> preload_slices =
        MakeSlices((*preload_ctx)->local_buffer, FLAGS_small_get_bytes);
    ReplicateConfig replicate_config;
    replicate_config.replica_num = 1;

    std::vector<std::string> get_keys;
    get_keys.reserve(FLAGS_preload_get_objects);
    for (int i = 0; i < FLAGS_preload_get_objects; ++i) {
        std::string key = scenario.output_stem + "-small-" + std::to_string(i);
        auto put =
            (*preload_ctx)->client->Put(key, preload_slices, replicate_config);
        if (!put.has_value()) {
            LOG(ERROR) << "Failed to preload small key=" << key
                       << " error=" << toString(put.error());
            return 1;
        }
        get_keys.push_back(std::move(key));
    }
    preload_ctx->reset();

    std::vector<std::thread> put_threads;
    std::vector<WorkerStats> put_stats(
        scenario.enable_background_put ? FLAGS_put_threads : 0);
    std::atomic<bool> put_stop{false};
    if (scenario.enable_background_put) {
        for (int i = 0; i < FLAGS_put_threads; ++i) {
            auto ctx = CreateWorkerClient(
                "127.0.0.1:" + std::to_string(client_ports[port_index++]),
                master_address, scenario.put_tenant_id, FLAGS_large_put_bytes);
            if (!ctx.has_value()) {
                return 1;
            }
            put_threads.emplace_back(PutWorker, std::move(ctx.value()), i,
                                     std::cref(scenario), std::ref(put_stop),
                                     std::ref(put_stats[i]));
        }
        std::this_thread::sleep_for(
            std::chrono::duration<double>(FLAGS_warmup_sec));
    }

    std::vector<std::thread> get_threads;
    std::vector<WorkerStats> get_stats(FLAGS_get_threads);
    std::atomic<bool> get_start{false};
    std::atomic<bool> get_stop{false};
    for (int i = 0; i < FLAGS_get_threads; ++i) {
        auto ctx = CreateWorkerClient(
            "127.0.0.1:" + std::to_string(client_ports[port_index++]),
            master_address, scenario.get_tenant_id, FLAGS_small_get_bytes);
        if (!ctx.has_value()) {
            put_stop.store(true);
            return 1;
        }
        get_threads.emplace_back(GetWorker, std::move(ctx.value()),
                                 std::cref(get_keys), std::ref(get_start),
                                 std::ref(get_stop), std::ref(get_stats[i]));
    }

    const auto begin = Clock::now();
    get_start.store(true, std::memory_order_release);
    std::this_thread::sleep_for(
        std::chrono::duration<double>(FLAGS_duration_sec));
    get_stop.store(true, std::memory_order_relaxed);
    for (auto& thread : get_threads) {
        thread.join();
    }
    const auto end = Clock::now();
    put_stop.store(true, std::memory_order_relaxed);
    for (auto& thread : put_threads) {
        thread.join();
    }

    const double measured_duration_sec =
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin)
            .count() /
        1e9;
    Summary summary =
        BuildSummary(scenario, get_stats, put_stats, measured_duration_sec);

    std::filesystem::create_directories(FLAGS_output_dir);
    std::filesystem::path output_dir(FLAGS_output_dir);
    WriteCsv(summary, output_dir / (scenario.output_stem + ".csv"));
    WriteJson(summary, output_dir / (scenario.output_stem + ".json"));
    LogSummary(summary);
    return 0;
}

}  // namespace
}  // namespace benchmark
}  // namespace mooncake

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    int rc = mooncake::benchmark::Run();
    google::ShutdownGoogleLogging();
    return rc;
}
