#include <grpc/grpc.h>
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
using serverless_learn::Worker;
using serverless_learn::Chunk;
using serverless_learn::ReceiveFileAck;


class WorkerStub {
 public:
  WorkerStub(std::shared_ptr<Channel> channel)
      : stub_(Worker::NewStub(channel)) {
    
  }

  void ReceiveFile() {
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
      std::cout << "sent" << std::endl;
    }
    writer->WritesDone();
    Status status = writer->Finish();
    if (status.ok() && ack.ok()) {
      std::cout << "ok"
                << std::endl;
    } else {
      std::cout << "not ok" << std::endl;
    }
  }

 private:
  std::unique_ptr<Worker::Stub> stub_;
};

int main(int argc, char** argv) {
  WorkerStub worker(
      grpc::CreateChannel("localhost:50051",
                          grpc::InsecureChannelCredentials()));
  worker.ReceiveFile();

  return 0;
}
