#include <chrono>
#include <fstream>
#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/resource_quota.h>
#include <grpcpp/support/channel_arguments.h>
#include <string>
#include <thread>
#include <unistd.h>

#include "benchmark.grpc.pb.h"

using grpc::Channel;
using grpc::ClientAsyncReaderWriter;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;

using benchmark::Ack;
using benchmark::Benchmark;
using benchmark::Data;

class BenchmarkClient
{
public:
    explicit BenchmarkClient(std::string bind_address)
    {
        grpc::ResourceQuota quota;
        quota.SetMaxThreads(4);

        grpc::ChannelArguments argument;
        argument.SetResourceQuota(quota);
        auto channel = grpc::CreateCustomChannel(
            bind_address, grpc::InsecureChannelCredentials(), argument);

        stub_ = Benchmark::NewStub(channel);
    }

    void Run(const std::string &str, int payload_size)
    {
        payload_size_ = payload_size;

        AsyncClientCall *call = new AsyncClientCall;
        call->stream = stub_->PrepareAsyncSendDataStreamFullDuplex(
            &call->context, &cq_);
        call->stream->StartCall((void *)call);
        call->sendfinished = false;
        call->finished = false;
        call->writing = true;

        PollCompletionQueue(str);
    }

    void PollCompletionQueue(const std::string &str)
    {
        void *tag;
        bool ok = false;

        long onehm = 1024 * 1024 * 100;
        long oneg = 1024 * 1024 * 1024;
        long packets_to_report = oneg / payload_size_;
        long packets_to_report_100m = onehm / payload_size_;

        int record_period = 1000;
        long tx_cnt = 0, rx_cnt = 0;
        size_t count = 0;

        auto start = std::chrono::system_clock::now();
        auto print_time = start;
        std::vector<double> latencies;

        while (cq_.Next(&tag, &ok))
        {
            AsyncClientCall *call = static_cast<AsyncClientCall *>(tag);
            if (not ok)
            {
                std::cout << "call = " << call << std::endl;
                std::cout << "count " << count << std::endl;
                std::cout << "sendfinished " << call->sendfinished << std::endl;
                std::cout << "finished " << call->finished << std::endl;
            }

            GPR_ASSERT(ok);

            if (call->writing == true)
            {
                Data d;
                d.set_data(str);
                call->writing = false;
                call->stream->Write(d, (void *)call);
                tx_cnt++;
                continue;
            }
            else
            {
                call->writing = true;
                call->stream->Read(&call->ack, (void *)call);
                rx_cnt++;
            }
            count++;

            auto end = std::chrono::system_clock::now();
            std::chrono::duration<double> diff = end - start;
            latencies.push_back(1e6*diff.count());
            std::chrono::duration<double> seconds = end - print_time;
            if (seconds.count() >= 1)
            {
                std::sort(latencies.begin(), latencies.end());
                size_t len = latencies.size();
                printf("%zu,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f "
                       "[%zu new samples, %zu total samples]\n",
                       (size_t)payload_size_,
                       latencies[(int)(0.5 * len)],
                       latencies[(int)(0.05 * len)],
                       latencies[(int)(0.99 * len)],
                       latencies[(int)(0.999 * len)],
                       latencies[(int)(0.9999 * len)],
                       latencies[(int)(0.99999 * len)],
                       latencies[(int)(0.999999 * len)],
                       latencies[len - 1],
                       len, count);
                print_time = end;
                latencies.clear();
            }
            start = std::chrono::system_clock::now();
        }
    }

private:
    struct AsyncClientCall
    {
        Ack ack;
        ClientContext context;
        Status status;
        std::unique_ptr<ClientAsyncReaderWriter<Data, Ack>> stream;
        bool sendfinished;
        bool finished;
        bool writing;
    };

    int payload_size_;

    std::unique_ptr<Benchmark::Stub> stub_;
    CompletionQueue cq_;
};

int main(int argc, char **argv)
{
    std::string bind_address = std::string(argv[1]);
    size_t payload_size = std::stoi(argv[2]);

    std::cout << "bind_address: " << bind_address << std::endl;
    std::cout << "payload_size: " << payload_size << std::endl;

    std::string a;
    a.assign(payload_size, 'a');

    BenchmarkClient client(bind_address);
    client.Run(a, payload_size);

    return 0;
}
