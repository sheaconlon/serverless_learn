#include <thread>
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

/* An implementation of the worker API (gRPC service) from the proto file.
 * See that for details. */
class WorkerImpl final : public Worker::Service {
 public:
  /* Construct a WorkerImpl. */
  explicit WorkerImpl() {
    
  }

  /* Implements Worker#ReceiveFile from the proto file. See that for details. */
  Status ReceiveFile(ServerContext* context, ServerReader<Chunk>* reader,
                     ReceiveFileAck* ack) override {
    std::cout << "receiving file" << std::endl;
    Chunk chunk;

    while (reader->Read(&chunk)) {
      
    }

    ack->set_ok(true);
    std::cout << "received file" << std::endl;
    return Status::OK;
  }

 private:

};

/* A stub for communicating with a master via its API (gRPC service). */
class MasterStub {
 public:
  MasterStub(std::shared_ptr<Channel> channel)
      : stub_(Master::NewStub(channel)) {
    
  }

  /* Tell the worker to register a birth
   *
   * Currently tells the master about our birth. */
  void RegisterBirth(std::string addr) {
    std::cout << "sending birth" << std::endl;
    WorkerBirthInfo birth;
    RegisterBirthAck ack;
    ClientContext context;
    birth.set_ip(addr);
    Status status = stub_->RegisterBirth(&context, birth, &ack);
    if (status.ok() && ack.ok()) {
      std::cout << "birth send succeeded" << std::endl;
    } else {
      std::cout << "birth send failed" << std::endl;
    }
  }

 private:
  std::unique_ptr<Master::Stub> stub_;
};

/* Serve requests to the worker API (gRPC service). */
void run_service(std::string addr) {
  std::string server_address(addr);
  std::cout << "starting service at " << server_address << std::endl;
  WorkerImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "service started" << std::endl;
  server->Wait();
}

int main(int argc, char** argv) {
  // Parse arguments.
  if (argc != 2) {
    std::cerr << "usage: worker ADDR" << std::endl;
    exit(1);
  }
  std::string addr(argv[1]);

  // Run the server in another thread. 
  std::thread service_thread(run_service, addr);

  // Register our birth.
  MasterStub master(
      grpc::CreateChannel(MASTER_ADDR,
                          grpc::InsecureChannelCredentials()));
  master.RegisterBirth(addr);

  // Wait for the server.
  service_thread.join();

  return 0;
}
