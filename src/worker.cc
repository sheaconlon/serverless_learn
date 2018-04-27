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
using serverless_learn::FlowFeedback;
using serverless_learn::PeerList;

std::vector<std::string> peer_list;

std::mutex peer_list_mutex;

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

  /* Responds to Master requests to see if Worker still alive. */
  Status CheckUp(ServerContext* context, const PeerList* peer_list,
                 FlowFeedback* feedback) override {
    std::cout << "responding to CheckUp" << std::endl;

    peer_list_mutex.lock()
    peer_list.clear();
    for (std::string peer : peer_list->peer_addrs()) {
      peer_list.push_back(peer);
    }
    peer_list_mutex.unlock();

    std::cout << "responded to CheckUp" << std::endl;
    return Status::OK;
  }

    /* Implements Master#RegisterBirth from the proto file. See that for
   * details. */
  Status ExchangeUpdates(ServerContext* context, const Update* update,
                       Update* return_update) override {
    std::cout << "updating the features" << std::endl;

    int remote_len = update->delta_size();
    while(model_state.size() < remote_len){
      model_state.push_back(0);
      old_state.push_back(0);
    }

    for (int i = 0; i < model_state.size();i++){
      model_state[i] += LEARN_RATE * updates->delta()[i];
      return_update->add_delta(model_state[i] - old_state[i]);  
    }

    old_state = model_state;

    std::cout << "end feature updates" << std::endl;
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
    birth.set_addr(addr);
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


/* A stub for communicating with a worker via its API (gRPC service). */
class WorkerStub {
 public:
  /* Construct a WorkerStub. */
  WorkerStub(std::shared_ptr<Channel> channel, int id)
      : stub_(Worker::NewStub(channel)), id(id) {

  }

  void ExchangeUpdates(Update* update){
    std::cout << "updating on worker" << id << std::endl;
    // PeerList peer_list;
    Update update_back;
    ClientContext context;



    Status status = stub_->ExchangeUpdates(&context, *update, &update_back);

    int remote_len = update_back->delta_size();
    while(model_state.size() < remote_len){
      model_state.push_back(0);
      old_state.push_back(0);
    }

    for (int i = 0; i < model_state.size();i++){
      model_state[i] += LEARN_RATE * update_back->delta()[i];
      return_update->add_delta(model_state[i] - old_state[i]);  
    }

    if (status.ok()) {
      std::cout << "update up on worker" << id << " succeeded" << std::endl;
    } else {
      std::cout << "update up on worker" << id << " failed" << std::endl;
    }

  }

 private:
  std::unique_ptr<Worker::Stub> stub_;
  int id;
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

void periodically_gossip() {

  while (true) {
    

    peer_list_mutex.lock();
    int lucky_worker_index = rand() % peer_list.size();
    std::string lucky_worker_addr = peer_list[lucky_worker_index];
    peer_list_mutex.unlock();


    Update update;
    for (int i = 0; i < model_state.size();i++){
      update->add_delta(model_state[i] - old_state[i]);  
    }

    auto channel = grpc::CreateChannel(lucky_worker_addr, grpc::InsecureChannelCredentials());
    WorkerStub worker(channel, lucky_worker_index);
    worker.ExchangeUpdates(&update);


    old_state = model_state;

    std::this_thread::sleep_for(std::chrono::milliseconds(GOSSIP_INTERVAL));
  }
}

void simulate_training () {
    while (true) {
    

    for (int i = 0; i < model_state.size(); i++) {
      model_state[i] = model_state[i] + 1;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(SIMULATED_TRAIN_INTERVAL));
  }
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

  std::thread gossip_thread(periodically_gossip);

  std::thread simulated_training_thread(simulate_training);

  // Register our birth.
  MasterStub master(
      grpc::CreateChannel(MASTER_ADDR,
                          grpc::InsecureChannelCredentials()));
  master.RegisterBirth(addr);

  // Wait for the server.
  service_thread.join();

  return 0;
}
