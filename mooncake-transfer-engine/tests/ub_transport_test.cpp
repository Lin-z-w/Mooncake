// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <sys/time.h>

#include <cstdlib>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string>
#include <thread>

#include "transfer_engine.h"
#include "transport/transport.h"
#include "common.h"

using namespace mooncake;

namespace mooncake {

DEFINE_string(local_server_name, getHostname(),
              "Local server name for segment discovery");
DEFINE_string(metadata_server, "127.0.0.1:2379", "etcd server host address");
DEFINE_string(mode, "initiator",
              "Running mode: initiator or target. Initiator node read/write "
              "data blocks from target node");
DEFINE_string(operation, "read", "Operation type: read or write");

DEFINE_string(protocol, "ub", "Transfer protocol: ub");

DEFINE_string(device_name, "mock_urma_device",
              "Device name to use, valid if protocol=ub");
DEFINE_string(nic_priority_matrix, "",
              "Path to UB NIC priority matrix file (Advanced)");

DEFINE_string(segment_id, "127.0.0.1", "Segment ID to access data");
DEFINE_int32(transfer_timeout_ms, 30000,
             "Timeout for each UB transfer test iteration");

std::string transferStatusToString(TransferStatusEnum status) {
    switch (status) {
        case TransferStatusEnum::WAITING:
            return "WAITING";
        case TransferStatusEnum::PENDING:
            return "PENDING";
        case TransferStatusEnum::INVALID:
            return "INVALID";
        case TransferStatusEnum::CANCELED:
            return "CANCELED";
        case TransferStatusEnum::COMPLETED:
            return "COMPLETED";
        case TransferStatusEnum::TIMEOUT:
            return "TIMEOUT";
        case TransferStatusEnum::FAILED:
            return "FAILED";
    }
    return "UNKNOWN";
}

::testing::AssertionResult waitForTransfer(TransferEngine *engine,
                                           BatchID batch_id, size_t task_id,
                                           const std::string &operation) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(FLAGS_transfer_timeout_ms);
    TransferStatus status{TransferStatusEnum::WAITING, 0};
    Status s;
    while (std::chrono::steady_clock::now() < deadline) {
        s = engine->getTransferStatus(batch_id, task_id, status);
        if (!s.ok()) {
            return ::testing::AssertionFailure()
                   << operation
                   << " getTransferStatus failed: " << s.ToString();
        }
        if (status.s == TransferStatusEnum::COMPLETED) {
            return ::testing::AssertionSuccess();
        }
        if (status.s == TransferStatusEnum::FAILED) {
            return ::testing::AssertionFailure()
                   << operation << " failed after transferring "
                   << status.transferred_bytes << " bytes";
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    auto &batch = Transport::toBatchDesc(batch_id);
    auto &task = batch.task_list[task_id];
    return ::testing::AssertionFailure()
           << operation << " timed out after " << FLAGS_transfer_timeout_ms
           << " ms; last status=" << transferStatusToString(status.s)
           << ", transferred_bytes=" << status.transferred_bytes
           << ", slices=" << task.slice_count
           << ", success_slices=" << task.success_slice_count
           << ", failed_slices=" << task.failed_slice_count;
}

std::string formatDeviceNames(const std::string &device_names) {
    std::stringstream ss(device_names);
    std::string item;
    std::vector<std::string> tokens;
    while (getline(ss, item, ',')) {
        tokens.push_back(item);
    }

    std::string formatted;
    for (size_t i = 0; i < tokens.size(); ++i) {
        formatted += "\"" + tokens[i] + "\"";
        if (i < tokens.size() - 1) {
            formatted += ",";
        }
    }
    return formatted;
}

std::string loadNicPriorityMatrix() {
    if (!FLAGS_nic_priority_matrix.empty()) {
        std::ifstream file(FLAGS_nic_priority_matrix);
        if (file.is_open()) {
            std::string content((std::istreambuf_iterator<char>(file)),
                                std::istreambuf_iterator<char>());
            file.close();
            return content;
        }
    }
    // Build JSON Data
    auto device_names = formatDeviceNames(FLAGS_device_name);
    return "{\"cpu:0\": [[" + device_names +
           "], []], "
           " \"cpu:1\": [[" +
           device_names +
           "], []], "
           " \"cuda:0\": [[" +
           device_names +
           "], []], "
           " \"musa:0\": [[" +
           device_names + "], []]}";
}

static void *allocateMemoryPool(size_t size, int socket_id,
                                bool from_vram = false) {
    return numa_alloc_onnode(size, socket_id);
}

static void freeMemoryPool(void *addr, size_t size) { numa_free(addr, size); }

class UBTransportTest : public ::testing::Test {
   public:
    std::shared_ptr<mooncake::TransferMetadata> metadata_client;
    void *addr = nullptr;
    std::pair<std::string, uint16_t> hostname_port;
    std::unique_ptr<mooncake::TransferEngine> engine;
    const size_t ram_buffer_size = 1ull << 30;
    Transport *xport;
    void **args;
    mooncake::Transport::SegmentID segment_id;
    std::shared_ptr<TransferMetadata::SegmentDesc> segment_desc;
    uint64_t remote_base;

   protected:
    void SetUp() override {
        static int offset = 0;
        google::InitGoogleLogging("UBTransportTest");
        FLAGS_logtostderr = 1;
        // disable topology auto discovery for testing.
        engine = std::make_unique<TransferEngine>(false);
        hostname_port = parseHostNameWithPort(FLAGS_local_server_name);
        engine->init(FLAGS_metadata_server, FLAGS_local_server_name.c_str(),
                     hostname_port.first.c_str(),
                     hostname_port.second + offset++);
        xport = nullptr;
        std::string nic_priority_matrix = loadNicPriorityMatrix();
        args = (void **)malloc(2 * sizeof(void *));
        args[0] = (void *)nic_priority_matrix.c_str();
        args[1] = nullptr;
        xport = engine->installTransport("ub", args);
        ASSERT_NE(xport, nullptr);
        addr = allocateMemoryPool(ram_buffer_size, 0, false);
        int rc = engine->registerLocalMemory(addr, ram_buffer_size, "cpu:0");
        ASSERT_EQ(rc, 0);
        // For testing purposes, we'll use the local memory address directly
        // instead of trying to open a remote segment
        segment_id = 0;  // Dummy segment ID for testing
        remote_base = (uint64_t)addr;
    }

    void TearDown() override {
        google::ShutdownGoogleLogging();
        engine->unregisterLocalMemory(addr);
        freeMemoryPool(addr, ram_buffer_size);
        if (args) {
            free(args);
        }
    }
};

TEST_F(UBTransportTest, MultiWrite) {
    const size_t kDataLength = 4096000;
    int times = 10;
    while (times--) {
        for (size_t offset = 0; offset < kDataLength; ++offset)
            *((char *)(addr) + offset) = 'a' + lrand48() % 26;
        auto batch_id = engine->allocateBatchID(1);
        Status s;
        TransferRequest entry;
        entry.opcode = TransferRequest::WRITE;
        entry.length = kDataLength;
        entry.source = (uint8_t *)(addr);
        entry.target_id = segment_id;
        entry.target_offset = remote_base;
        s = engine->submitTransfer(batch_id, {entry});
        LOG_ASSERT(s.ok());
        ASSERT_TRUE(waitForTransfer(engine.get(), batch_id, 0, "MultiWrite"));
        s = engine->freeBatchID(batch_id);
        ASSERT_EQ(s, Status::OK());
    }
}

TEST_F(UBTransportTest, MultipleRead) {
    const size_t kDataLength = 4096000;
    int times = 10;
    while (times--) {
        for (size_t offset = 0; offset < kDataLength; ++offset)
            *((char *)(addr) + offset) = 'a' + lrand48() % 26;

        auto batch_id = engine->allocateBatchID(1);
        Status s;
        TransferRequest entry;
        entry.opcode = TransferRequest::WRITE;
        entry.length = kDataLength;
        entry.source = (uint8_t *)(addr);
        entry.target_id = segment_id;
        entry.target_offset = remote_base;
        s = engine->submitTransfer(batch_id, {entry});
        LOG_ASSERT(s.ok());
        ASSERT_TRUE(
            waitForTransfer(engine.get(), batch_id, 0, "MultipleRead.Write"));
        s = engine->freeBatchID(batch_id);
        ASSERT_EQ(s, Status::OK());
    }
    times = 10;
    while (times--) {
        auto batch_id = engine->allocateBatchID(1);

        TransferRequest entry;
        entry.opcode = TransferRequest::READ;
        entry.length = kDataLength;
        entry.source = (uint8_t *)(addr) + kDataLength;
        entry.target_id = segment_id;
        entry.target_offset = remote_base;
        Status s;
        s = engine->submitTransfer(batch_id, {entry});
        ASSERT_EQ(s, Status::OK());
        ASSERT_TRUE(
            waitForTransfer(engine.get(), batch_id, 0, "MultipleRead.Read"));
        s = engine->freeBatchID(batch_id);
        ASSERT_EQ(s, Status::OK());
    }
}

}  // namespace mooncake

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
