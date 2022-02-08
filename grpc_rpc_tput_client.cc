#include <chrono>
#include <fstream>
#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/resource_quota.h>
#include <grpcpp/support/channel_arguments.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <atomic>

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
    explicit BenchmarkClient(std::string bind_address, int num_threads, size_t grpc_max_msgsize)
    {

        for (int i = 0; i < num_threads; i++)
        {
            grpc::ResourceQuota quota;
            quota.SetMaxThreads(4);

            grpc::ChannelArguments argument;
            argument.SetResourceQuota(quota);
            // argument.SetMaxSendMessageSize(grpc_max_msgsize * 1024 * 1024);
            argument.SetMaxReceiveMessageSize(grpc_max_msgsize * 1024 * 1024);
            auto channel = grpc::CreateCustomChannel(
                bind_address, grpc::InsecureChannelCredentials(), argument);

            stubs_.push_back(Benchmark::NewStub(channel));
        }
        num_threads_ = num_threads;
    }

    void Run(const std::string &str, int payload_size, int concurrency)
    {

        payload_size_ = payload_size;

        stats_ = std::vector<Stat>(num_threads_);
        for (int i = 0; i < num_threads_; i++)
            cqs_.push_back(new CompletionQueue());

        polling_threads_.reserve(num_threads_);
        for (int i = 0; i < num_threads_; i++)
        {
            polling_threads_.emplace_back(&BenchmarkClient::PollCompletionQueue, this,
                                          i, str);
        }

        for (int id = 0; id < num_threads_; id++)
        {
            for (int i = 0; i < concurrency; i++)
            {
                AsyncClientCall *call = new AsyncClientCall;
                call->stream = stubs_[id]->PrepareAsyncSendDataStreamFullDuplex(
                    &call->context, cqs_[id]);
                call->stream->StartCall((void *)call);
                call->sendfinished = false;
                call->finished = false;
                call->writing = true;
            }
        }

        for (int i = 0; i < num_threads_; i++)
            polling_threads_[i].join();
    }

    void PollCompletionQueue(int id, const std::string &str)
    {
        void *tag;
        bool ok = false;

        long onehm = 1024 * 1024 * 100;
        long oneg = 1024 * 1024 * 1024;
        long packets_to_report = oneg / payload_size_;
        long packets_to_report_100m = onehm / payload_size_;

        int count = 0, record_period = 100;
        Stat &stat = stats_[id];
        stat.rx_cnt = 0;
        stat.tx_cnt = 0;

        auto start = std::chrono::system_clock::now();
        if (id == 0)
            printf("rx_gbps,\t tx_gbps,\t rx_rps,\t tx_rps\n");

        while (cqs_[id]->Next(&tag, &ok))
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
                stat.tx_cnt++;
            }
            else
            {
                call->writing = true;
                call->stream->Read(&call->ack, (void *)call);
                stat.rx_cnt++;
            }

            if (id > 0)
                continue;

            count++;
            // std::cout << count << std::endl;
            if (count >= record_period)
            {
                auto end = std::chrono::system_clock::now();
                std::chrono::duration<double> seconds = end - start;
                double duration = seconds.count();
                if (duration >= 1)
                {
                    long rx_cnt_acc = 0, tx_cnt_acc = 0;
                    for (int i = 0; i < num_threads_; i++)
                    {
                        rx_cnt_acc += stats_[i].rx_cnt;
                        tx_cnt_acc += stats_[i].tx_cnt;
                        stats_[i].rx_cnt = 0;
                        stats_[i].tx_cnt = 0;
                    }
                    double rx_gbps = 8.0 * rx_cnt_acc * payload_size_ / duration / 1e9;
                    double tx_gbps = 8.0 * tx_cnt_acc * payload_size_ / duration / 1e9;
                    double rx_rps = rx_cnt_acc / duration;
                    double tx_rps = tx_cnt_acc / duration;
                    printf("%.6lf,\t %.6lf,\t %.1lf,\t %.1lf\n",
                           rx_gbps, tx_gbps, rx_rps, tx_rps);
                    start = std::chrono::system_clock::now();
                }
                count = 0;
            }
        }
    }

    ~BenchmarkClient()
    {
        for (auto cq : cqs_)
            delete cq;
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

    struct Stat
    {
        std::atomic<long> rx_cnt, tx_cnt;
    };

    int num_threads_;
    int payload_size_;
    std::vector<Stat> stats_;
    // std::mutex mtx;

    std::vector<std::unique_ptr<Benchmark::Stub>> stubs_;
    std::vector<CompletionQueue *> cqs_;
    std::vector<std::thread> polling_threads_;
};

int main(int argc, char **argv)
{
    std::string bind_address = std::string(argv[1]);
    size_t payload_size = std::stoi(argv[2]);
    int num_threads = std::stoi(argv[3]);
    int concurrency = std::stoi(argv[4]);
    int grpc_max_msgsize = std::max(4, int(payload_size / 1024.0 / 1024.0) + 1);
    if (argc > 5)
        grpc_max_msgsize = std::stoi(argv[5]);

    std::cout << "bind_address: " << bind_address << std::endl;
    std::cout << "payload_size: " << payload_size << std::endl;
    std::cout << "num_threads: " << num_threads << std::endl;
    std::cout << "concurrency (per thread): " << concurrency << std::endl;
    std::cout << "grpc_max_msgsize: " << grpc_max_msgsize << "MB" << std::endl;

    std::string a;
    a.assign(payload_size, 'a');

    BenchmarkClient client(bind_address, num_threads, grpc_max_msgsize);

    client.Run(a, payload_size, concurrency);

    return 0;
}
