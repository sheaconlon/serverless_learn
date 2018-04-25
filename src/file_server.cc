#include <thread>
#include <random>
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
using serverless_learn::FileServer;
using serverless_learn::Push;
using serverless_learn::PushOutcome;
using serverless_learn::LoadFeedback;
using serverless_learn::Empty;

/* The number of bytes that the dummy file (below) should contain. */
const long DUMMY_FILE_LENGTH = 100 * 1000 * 1000;

/* A dummy file, file number 0. */
std::string file;

/* The number of bytes of data that each chunk should contain. */
const long CHUNK_SIZE = 1 * 1000 * 1000;

/* A stub for communicating with a worker via its API (gRPC service). */
class WorkerStub {
 public:
  /* Construct a WorkerStub. */
  WorkerStub(std::shared_ptr<Channel> channel)
      : stub_(Worker::NewStub(channel)) {

  }

  /* Tell the worker to receive a file.
   *
   * Currently streams the dummy file. */
  bool ReceiveFile() {
    std::cout << "sending file" << std::endl;
    Chunk chunk;
    ReceiveFileAck ack;
    ClientContext context;

    std::unique_ptr<ClientWriter<Chunk>> writer(
        stub_->ReceiveFile(&context, &ack));
    long pos = 0;
    while (pos < file.length()) {
      std::string data = file.substr(pos, CHUNK_SIZE);
      chunk.set_data(data);
      if (!writer->Write(chunk)) {
        break;
      }
      pos += data.length();
    }
    writer->WritesDone();
    Status status = writer->Finish();
    if (status.ok() && ack.ok()) {
      std::cout << "file send succeeded" << std::endl;
      return true;
    } else {
      std::cerr << "file send failed" << std::endl;
      std::cerr << status.error_message() << std::endl;
      return false;
    }
  }

 private:
  std::unique_ptr<Worker::Stub> stub_;
};

/* An implementation of the file server API (gRPC service) from the proto file.
 * See that for details. */
class FileServerImpl final : public FileServer::Service {
 public:
  /* Construct a FileServerImpl. */
  explicit FileServerImpl() {

  }

  /* Implements FileServer#DoPush from the proto file. See that for details. */
  Status DoPush(ServerContext* context, const Push* push,
                PushOutcome* outcome) override {
    std::cout << "doing push" << std::endl;

    if (push->file_num() != 0) {
      std::cerr << "received push with file_number not 0" << std::endl;
      exit(1);
    }
    WorkerStub worker(
        grpc::CreateChannel(push->recipient_addr(),
                              grpc::InsecureChannelCredentials()));
    bool ok = worker.ReceiveFile();

    outcome->set_ok(ok);
    std::cout << "did push" << std::endl;
    return Status::OK;
  }

  /* Responds to Master requests to see if FileServer still alive. */
  Status CheckUp(ServerContext* context, const Empty* empty,
                 LoadFeedback* feedback) override {
    std::cout << "responding to CheckUp" << std::endl;

    // TODO

    std::cout << "responded to CheckUp" << std::endl;
    return Status::OK;
  }

 private:

};

/* Serve requests to the file server API (gRPC service). */
void serve_requests() {
  std::string server_address(FILE_SERVER_ADDR);
  std::cout << "starting service at " << server_address << std::endl;
  FileServerImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "service started" << std::endl;
  server->Wait();
}

int main(int argc, char** argv) {
  // Fill the dummy file.
 std::independent_bits_engine<
    std::default_random_engine, CHAR_BIT, unsigned char> generator;
  for (int i = 0; i < DUMMY_FILE_LENGTH; i++) {
    file += generator();
  }

  // Run the server in another thread.
  std::thread server_thread(serve_requests);

  // Wait for the server.
  server_thread.join();

  return 0;
}
