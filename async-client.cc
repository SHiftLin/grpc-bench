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
    explicit BenchmarkClient(std::string bind_address, int num_threads)
    {

        for (int i = 0; i < num_threads; i++)
        {
            grpc::ResourceQuota quota;
            quota.SetMaxThreads(4);

            grpc::ChannelArguments argument;
            argument.SetResourceQuota(quota);
            auto channel = grpc::CreateCustomChannel(
                bind_address, grpc::InsecureChannelCredentials(), argument);

            stubs_.push_back(Benchmark::NewStub(channel));
        }
        num_threads_ = num_threads;
    }

    void Run(const std::string &str, int payload_size)
    {

        payload_size_ = payload_size;
        int max_outgoing = 100;

        polling_threads_.reserve(num_threads_);
        for (int i = 0; i < num_threads_; i++)
        {
            cqs_.push_back(new CompletionQueue());
        }

        for (int i = 0; i < num_threads_; i++)
        {
            polling_threads_.emplace_back(&BenchmarkClient::PollCompletionQueue, this,
                                          i, str);
        }

        for (int i = 0; i < max_outgoing; i++)
        {
            AsyncClientCall *call = new AsyncClientCall;
            int id = rand() % num_threads_;
            call->stream = stubs_[id]->PrepareAsyncSendDataStreamFullDuplex(
                &call->context, cqs_[id]);
            call->count = 0;
            call->stream->StartCall((void *)call);
            call->sendfinished = false;
            call->finished = false;
            call->count = 0;
            call->writing = true;
        }

        for (int i = 0; i < num_threads_; i++)
        {
            polling_threads_[i].join();
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
        std::cout << packets_to_report_100m << std::endl;

        long c = 0;

        auto start = std::chrono::system_clock::now();

        while (cqs_[id]->Next(&tag, &ok))
        {
            AsyncClientCall *call = static_cast<AsyncClientCall *>(tag);
            if (not ok)
            {
                std::cout << "call = " << call << std::endl;
                std::cout << "count " << call->count << std::endl;
                std::cout << "sendfinished " << call->sendfinished << std::endl;
                std::cout << "finished " << call->finished << std::endl;
            }

            GPR_ASSERT(ok);

            if ((call->count < 10) && (call->writing == true))
            {
                Data d;
                d.set_data(str);
                call->writing = false;
                call->stream->Write(d, (void *)call);
                call->count++;
                // std::cout<<"write"<<std::endl;
                continue;
            }

            if ((call->count <= 10) && (call->writing == false))
            {
                call->writing = true;
                call->stream->Read(&call->ack, (void *)call);
                continue;
            }

            if (not call->sendfinished)
            {
                call->stream->WritesDone((void *)call);
                call->sendfinished = true;
                continue;
            }

            if (not call->finished)
            {
                call->stream->Finish(&call->status, (void *)call);
                call->finished = true;
                continue;
            }

            if (call->status.ok())
            {
                c += 10;
                if (c > packets_to_report_100m)
                {
                    auto end = std::chrono::system_clock::now();
                    std::chrono::duration<double> diff = end - start;
                    double gbps = 8.0 * onehm / diff.count() / 1e9;
                    std::cout << id << ": " << gbps << " Gbps" << std::endl;
                    double rps = c / diff.count();
                    std::cout << rps << std::endl;
                    start = end;
                    c = 0;
                }
            }
            else
            {
                std::cout << "RPC failed" << std::endl;
            }
            delete call;

            call = new AsyncClientCall;
            call->stream = stubs_[id]->PrepareAsyncSendDataStreamFullDuplex(
                &call->context, cqs_[id]);
            call->stream->StartCall((void *)call);
            call->sendfinished = false;
            call->finished = false;
            call->count = 0;
            call->writing = true;
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
    };

    int num_threads_;
    int payload_size_;

    std::vector<std::unique_ptr<Benchmark::Stub>> stubs_;
    std::vector<CompletionQueue *> cqs_;
    std::vector<std::thread> polling_threads_;
};

int main(int argc, char **argv)
{
    size_t num_threads = std::stoi(argv[1]);
    std::string ip = std::string(argv[2]);
    int port = std::stoi(argv[3]);
    size_t payload_size = std::stoi(argv[4]);
    std::cout << num_threads << " " << ip << " " << port << " " << payload_size << std::endl;

    std::string a;
    a.assign(payload_size, 'a');

    auto bind_address = ip + ":" + std::to_string(port);

    BenchmarkClient client(bind_address, num_threads);

    client.Run(a, payload_size);

    return 0;
}
