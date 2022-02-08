#include <fstream>
#include <grpc/grpc.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <iostream>
#include <string>
#include <thread>
#include <unistd.h>

#include "benchmark.grpc.pb.h"

using grpc::Server;
using grpc::ServerAsyncReaderWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::Status;

using benchmark::Ack;
using benchmark::Benchmark;
using benchmark::Data;

class BenchmarkAsyncImpl final
{
public:
    ~BenchmarkAsyncImpl()
    {
        server_->Shutdown();
        for (size_t i = 0; i < cqs_.size(); i++)
        {
            cqs_[i]->Shutdown();
        }
    }

    void Run(std::string server_address, int port, size_t num_threads, size_t grpc_max_msgsize)
    {
        ServerBuilder builder;
        std::string bind_address = server_address + ":" + std::to_string(port);
        builder.AddListeningPort(bind_address, grpc::InsecureServerCredentials());
        // builder.SetMaxSendMessageSize(grpc_max_msgsize * 1024 * 1024);
        builder.SetMaxReceiveMessageSize(grpc_max_msgsize * 1024 * 1024);

        builder.RegisterService(&service_);
        for (size_t i = 0; i < num_threads; i++)
        {
            cqs_.push_back(std::move(builder.AddCompletionQueue()));
        }
        server_ = builder.BuildAndStart();
        std::cout << "Async server listening on " << server_address << std::endl;

        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        for (size_t i = 0; i < num_threads; i++)
        {
            threads.emplace_back(HandleRPCs, &service_, cqs_[i].get());
        }

        for (size_t i = 0; i < num_threads; i++)
        {
            threads[i].join();
        }
    }

private:
    class HandleRPC
    {
    public:
        HandleRPC(Benchmark::AsyncService *service, ServerCompletionQueue *cq)
            : service_(service), cq_(cq), stream_(&ctx_), status_(CREATE)
        {
            ack_.set_data(std::string(4, 'b'));
            P(nullptr, true);
        }

        void P(std::string *d, bool ok)
        {
            if (status_ == CREATE)
            {
                status_ = PROCESS;
                service_->RequestSendDataStreamFullDuplex(&ctx_, &stream_, cq_, cq_, this);
            }
            else if (status_ == PROCESS)
            {
                new HandleRPC(service_, cq_);
                status_ = PROCESSING_READ;
                stream_.Read(&data_, this);
            }
            else if (status_ == PROCESSING_READ)
            {
                d->assign(data_.data());
                data_.Clear();
                if (ok)
                {
                    status_ = PROCESSING_WRITE;
                    stream_.Write(ack_, this);
                }
                else
                {
                    status_ = FINISH;
                    stream_.Finish(Status::OK, this);
                }
            }
            else if (status_ == PROCESSING_WRITE)
            {
                if (ok)
                {
                    status_ = PROCESSING_READ;
                    stream_.Read(&data_, this);
                }
                else
                {
                    status_ = FINISH;
                    stream_.Finish(Status::OK, this);
                }
            }
            else
            {
                GPR_ASSERT(status_ == FINISH);
                delete this;
            }
        }

    private:
        Benchmark::AsyncService *service_;
        ServerCompletionQueue *cq_;
        ServerContext ctx_;

        Data data_;
        Ack ack_;

        ServerAsyncReaderWriter<Ack, Data> stream_;

        enum CallStatus
        {
            CREATE,
            PROCESS,
            PROCESSING_READ,
            PROCESSING_WRITE,
            FINISH
        };
        CallStatus status_;
    };

    static void HandleRPCs(Benchmark::AsyncService *service,
                           ServerCompletionQueue *cq)
    {
        new HandleRPC(service, cq);
        void *tag;
        bool ok;
        std::string a;
        while (true)
        {
            GPR_ASSERT(cq->Next(&tag, &ok));
            static_cast<HandleRPC *>(tag)->P(&a, ok);
        }
    }

    std::vector<std::unique_ptr<ServerCompletionQueue>> cqs_;
    Benchmark::AsyncService service_;
    std::unique_ptr<Server> server_;
};

int main(int argc, char **argv)
{
    std::string ip = "0.0.0.0";
    int port = atoi(argv[1]);
    int num_threads = std::stoi(argv[2]);
    int grpc_max_msgsize = 4;
    if (argc > 3)
        grpc_max_msgsize = std::stoi(argv[3]);

    BenchmarkAsyncImpl server;
    server.Run(ip, port, num_threads, grpc_max_msgsize);
}
