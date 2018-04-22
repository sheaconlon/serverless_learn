#include <grpc/grpc.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/security/server_credentials.h>
#include "serverless_learn.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using serverless_learn::Worker;
using serverless_learn::Chunk;
using serverless_learn::ReceiveFileAck;


class WorkerImpl final : public Worker::Service {
 public:
  explicit WorkerImpl() {
    
  }

  Status ReceiveFile(ServerContext* context, ServerReader<Chunk>* reader,
                     ReceiveFileAck* ack) override {
    Chunk chunk;

    while (reader->Read(&chunk)) {
      std::cout << "received" << std::endl;
    }

    ack->set_ok(true);
    return Status::OK;
  }

 private:

};

int main(int argc, char** argv) {
  std::string server_address("0.0.0.0:50051");
  WorkerImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  server->Wait();

  return 0;
}
