#include <thread>
#include <mutex>
#include <grpc/grpc.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include "serverless_learn.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using serverless_learn::Master;
using serverless_learn::WorkerBirthInfo;
using serverless_learn::RegisterBirthAck;
using serverless_learn::Worker;
using serverless_learn::Chunk;
using serverless_learn::ReceiveFileAck;

typedef struct WorkerInfo {
    WorkerInfo(std::string ip) {
      this->ip = ip;
    }

    std::string ip;
} WorkerInfo;

std::vector<std::shared_ptr<WorkerInfo>> workers;
std::mutex workers_mutex;

class MasterImpl final : public Master::Service {
 public:
  explicit MasterImpl() {
    
  }

  Status RegisterBirth(ServerContext* context, const WorkerBirthInfo* birth,
                       RegisterBirthAck* ack) override {
    std::cout << "registering birth" << std::endl;

    workers_mutex.lock();
    std::shared_ptr<WorkerInfo> worker(new WorkerInfo(birth->ip()));
    workers.push_back(worker);
    workers_mutex.unlock();

    ack->set_ok(true);
    std::cout << "registered birth" << std::endl;
    return Status::OK;
  }

 private:

};

class WorkerStub {
 public:
  WorkerStub(std::shared_ptr<Channel> channel)
      : stub_(Worker::NewStub(channel)) {
    
  }

  void ReceiveFile() {
    std::cout << "sending file" << std::endl;
    Chunk chunk;
    ReceiveFileAck ack;
    ClientContext context;

    std::unique_ptr<ClientWriter<Chunk>> writer(
        stub_->ReceiveFile(&context, &ack));
    for (int i = 0; i < 100; i++) {
      chunk.set_data("Hello, world!");
      if (!writer->Write(chunk)) {
        break;
      }
    }
    writer->WritesDone();
    Status status = writer->Finish();
    if (status.ok() && ack.ok()) {
      std::cout << "file send succeeded" << std::endl;
    } else {
      std::cout << "file send failed" << std::endl;
    }
  }

 private:
  std::unique_ptr<Worker::Stub> stub_;
};

void run_service() {
  std::string server_address("0.0.0.0:50052");
  std::cout << "starting service at " << server_address << std::endl;
  MasterImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "service started" << std::endl;
  server->Wait();
}

int main(int argc, char** argv) {
  std::thread service_thread(run_service);

  while (true) {
    workers_mutex.lock();
    bool empty = workers.empty();
    std::shared_ptr<WorkerInfo> worker_info;
    if (!empty) {
      worker_info = workers.front();
    }
    workers_mutex.unlock();
    if (!empty) {
      WorkerStub worker(
          grpc::CreateChannel(worker_info->ip,
                              grpc::InsecureChannelCredentials()));
      worker.ReceiveFile();
    }
  }

  service_thread.join();
  return 0;
}
