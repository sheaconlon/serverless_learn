#include <thread>
#include <mutex>
#include <chrono>
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
#include "serverless_learn.h"

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

/* The number of milliseconds to wait between rounds of file pushing. */
const int FILE_PUSH_INTERVAL = 5000;

/* Information about a worker. */
typedef struct WorkerInfo {
    /* Construct a WorkerInfo. */
    WorkerInfo(std::string addr) {
      this->addr = addr;
    }

    std::string addr; // The worker's address (hostname and port).
} WorkerInfo;

/* Information about all currently-known, live workers. */
std::vector<std::shared_ptr<WorkerInfo>> workers;

/* A mutex to be used whenever accessing the above. */
std::mutex workers_mutex;

/* An implementation of the master API (gRPC service) from the proto file.
 * See that for details. */
class MasterImpl final : public Master::Service {
 public:
  /* Construct a MasterImpl. */
  explicit MasterImpl() {
    
  }

  /* Implements Master#RegisterBirth from the proto file. See that for
   * details. */
  Status RegisterBirth(ServerContext* context, const WorkerBirthInfo* birth,
                       RegisterBirthAck* ack) override {
    std::cout << "registering birth" << std::endl;

    workers_mutex.lock();
    std::shared_ptr<WorkerInfo> worker(new WorkerInfo(birth->addr()));
    workers.push_back(worker);
    workers_mutex.unlock();

    ack->set_ok(true);
    std::cout << "registered birth" << std::endl;
    return Status::OK;
  }

 private:

};

/* A stub for communicating with a worker via its API (gRPC service). */
class WorkerStub {
 public:
  /* Construct a WorkerStub. */
  WorkerStub(std::shared_ptr<Channel> channel)
      : stub_(Worker::NewStub(channel)) {
    
  }

  /* Tell the worker to receive a file.
   *
   * Currently streams 100 chunks that say "Hello, world!". */
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

/* Serve requests to the master API (gRPC service). */
void serve_requests() {
  std::string server_address(MASTER_ADDR);
  std::cout << "starting service at " << server_address << std::endl;
  MasterImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "service started" << std::endl;
  server->Wait();
}

/* Periodically push a file to each worker. */
void push_file() {
  while (true) {
    workers_mutex.lock();
    std::vector<std::string> worker_addrs;
    for (std::shared_ptr<WorkerInfo> worker_info : workers) {
      worker_addrs.push_back(worker_info->addr);
    }
    workers_mutex.unlock();
    for (std::string addr : worker_addrs) {
      WorkerStub worker(
          grpc::CreateChannel(addr,
                              grpc::InsecureChannelCredentials()));
      worker.ReceiveFile();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(FILE_PUSH_INTERVAL));
  }
}

int main(int argc, char** argv) {
  // Run the server in another thread.
  std::thread server_thread(serve_requests);

  // Periodically push files.
  push_file();

  // Wait for the server.
  server_thread.join();

  return 0;
}
