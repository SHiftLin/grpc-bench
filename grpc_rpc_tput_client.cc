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
#include <signal.h>

#include "benchmark.grpc.pb.h"

using grpc::Channel;
using grpc::ClientAsyncReaderWriter;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;

using benchmark::Ack;
using benchmark::Benchmark;
using benchmark::Data;

long overlimit = 0;

class Params
{
public:
    std::string address;
    size_t payload_size;
    int num_threads, concurrency;
    int grpc_max_msgsize, call_per_req;

    Params() : address("127.0.0.1:50051"), payload_size(32), num_threads(1), concurrency(1), grpc_max_msgsize(4), call_per_req(0)
    {
    }

    void Print()
    {
        std::cout << "address: " << address << std::endl;
        std::cout << "payload_size: " << payload_size << std::endl;
        std::cout << "num_threads: " << num_threads << std::endl;
        std::cout << "concurrency (per thread): " << concurrency << std::endl;
        std::cout << "grpc_max_msgsize: " << grpc_max_msgsize << "MB" << std::endl;
        std::cout << "call_per_req (0 is infinity): " << call_per_req << std::endl;
    }
};

class BenchmarkClient
{
public:
    explicit BenchmarkClient(const Params &params)
    {

        for (int i = 0; i < params.num_threads; i++)
        {
            grpc::ResourceQuota quota;
            quota.SetMaxThreads(4);

            grpc::ChannelArguments argument;
            argument.SetResourceQuota(quota);
            // argument.SetMaxSendMessageSize(params,grpc_max_msgsize * 1024 * 1024);
            argument.SetMaxReceiveMessageSize(params.grpc_max_msgsize * 1024 * 1024);
            auto channel = grpc::CreateCustomChannel(
                params.address, grpc::InsecureChannelCredentials(), argument);

            stubs_.push_back(Benchmark::NewStub(channel));
        }
        num_threads_ = params.num_threads;
        payload_size_ = params.payload_size;
        call_per_req_ = params.call_per_req;
    }

    void Run(const std::string &str, const Params &params)
    {
        stats_ = std::vector<Stat>(num_threads_);
        for (int i = 0; i < num_threads_; i++)
            cqs_.push_back(new CompletionQueue());

        stat_thread_ = std::thread(&BenchmarkClient::PollStats, this);

        polling_threads_.reserve(num_threads_);
        for (int id = 0; id < num_threads_; id++)
        {
            polling_threads_.emplace_back(&BenchmarkClient::PollCompletionQueue, this,
                                          id, str);
        }

        for (int id = 0; id < num_threads_; id++)
        {
            for (int i = 0; i < params.concurrency; i++)
                AsyncClientCall *call = new AsyncClientCall(stubs_[id], cqs_[id]);
        }

        for (int i = 0; i < num_threads_; i++)
            polling_threads_[i].join();
        stat_thread_.join();
    }

    void PollStats()
    {
        auto start = std::chrono::system_clock::now();
        while (true)
        {
            sleep(1);

            auto end = std::chrono::system_clock::now();
            std::chrono::duration<double> seconds = end - start;
            double duration = seconds.count();

            long rx_cnt_acc = 0, tx_cnt_acc = 0;
            for (int i = 0; i < num_threads_; i++)
            {
                rx_cnt_acc += stats_[i].rx_cnt;
                tx_cnt_acc += stats_[i].tx_cnt;
                stats_[i].rx_cnt = 0;
                stats_[i].tx_cnt = 0;
            }
            double rx_gbps = 8.0 * rx_cnt_acc * 4 / duration / 1e9;
            double tx_gbps = 8.0 * tx_cnt_acc * payload_size_ / duration / 1e9;
            double rx_rps = rx_cnt_acc / duration;
            double tx_rps = tx_cnt_acc / duration;
            printf("%.6lf,\t %.6lf,\t %.1lf,\t %.1lf\n",
                   rx_gbps, tx_gbps, rx_rps, tx_rps);
            start = std::chrono::system_clock::now();
        }
    }

    void PollCompletionQueue(int id, const std::string &str)
    {
        void *tag;
        bool ok = false;

        long onehm = 1024 * 1024 * 100;
        long oneg = 1024 * 1024 * 1024;
        long packets_to_report = oneg / payload_size_;
        long packets_to_report_100m = onehm / payload_size_;

        Stat &stat = stats_[id];
        stat.rx_cnt = 0;
        stat.tx_cnt = 0;

        if (id == 0)
            printf("rx_gbps,\t tx_gbps,\t rx_rps,\t tx_rps\n");

        while (cqs_[id]->Next(&tag, &ok))
        {
            AsyncClientCall *call = static_cast<AsyncClientCall *>(tag);

            // GPR_ASSERT(ok);
            if (GPR_UNLIKELY(!ok))
            {
                overlimit += 1;
                delete call;
                AsyncClientCall *call = new AsyncClientCall(stubs_[id], cqs_[id]);
                continue;
            }

            if ((call_per_req_ <= 0 || call->count < call_per_req_) && call->writing == true)
            {
                Data d;
                d.set_data(str);
                call->writing = false;
                call->stream->Write(d, (void *)call);
                stat.tx_cnt++;
                call->count++;
                continue;
            }

            if ((call_per_req_ <= 0 || call->count <= call_per_req_) && call->writing == false)
            {
                call->writing = true;
                call->stream->Read(&call->ack, (void *)call);
                stat.rx_cnt++;
                if (call_per_req_ > 0)
                    continue;
            }

            if (call_per_req_ > 0 && !call->sendfinished)
            {
                call->stream->WritesDone((void *)call);
                call->sendfinished = true;
                continue;
            }

            if (call_per_req_ > 0 && !call->finished)
            {
                call->stream->Finish(&call->status, (void *)call);
                call->finished = true;
                continue;
            }

            if (call_per_req_ > 0)
            {
                delete call;
                AsyncClientCall *call = new AsyncClientCall(stubs_[id], cqs_[id]);
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
        int count;
        bool sendfinished;
        bool finished;
        bool writing;

        // AsyncClientCall() = default;

        AsyncClientCall(const std::unique_ptr<Benchmark::Stub> &stub_, CompletionQueue *cq_)
        {
            this->stream = stub_->PrepareAsyncSendDataStreamFullDuplex(
                &this->context, cq_);
            this->stream->StartCall((void *)this);
            this->sendfinished = false;
            this->finished = false;
            this->writing = true;
            this->count = 0;
        }
    };

    struct Stat
    {
        std::atomic<long> rx_cnt, tx_cnt;
    };

    int num_threads_;
    int payload_size_;
    int call_per_req_;
    std::vector<Stat> stats_;

    std::vector<std::unique_ptr<Benchmark::Stub>> stubs_;
    std::vector<CompletionQueue *> cqs_;
    std::vector<std::thread> polling_threads_;
    std::thread stat_thread_;
};

void sigint_handler(int sig)
{
    std::cout << "overlimit: " << overlimit << std::endl;
    exit(0);
}

int main(int argc, char **argv)
{
    Params params;

    int op;
    while ((op = getopt(argc, argv, "s:n:c:m:r:")) != -1)
    {
        switch (op)
        {
        case 's':
            params.payload_size = std::stoi(optarg);
            break;
        case 'n':
            params.num_threads = std::stoi(optarg);
            break;
        case 'c':
            params.concurrency = std::stoi(optarg);
            break;
        case 'm':
            params.grpc_max_msgsize = std::stoi(optarg);
            break;
        case 'r':
            params.call_per_req = std::stoi(optarg);
            break;
        }
    }

    params.grpc_max_msgsize = std::max(params.grpc_max_msgsize, int(params.payload_size / 1024.0 / 1024.0) + 1);

    if (optind < argc)
        params.address = std::string(argv[optind]);

    params.Print();

    std::string a(params.payload_size, 'a');

    BenchmarkClient client(params);

    signal(SIGINT, sigint_handler);
    client.Run(a, params);

    return 0;
}
