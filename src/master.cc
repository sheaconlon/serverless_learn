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

#include <vector>

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
using serverless_learn::Worker;
using serverless_learn::WorkerBirthInfo;
using serverless_learn::RegisterBirthAck;
using serverless_learn::FileServer;
using serverless_learn::Push;
using serverless_learn::PushOutcome;
using serverless_learn::FlowFeedback;
using serverless_learn::LoadFeedback;
using serverless_learn::PeerList;
using serverless_learn::Empty;

/* The number of milliseconds to wait between rounds of file pushing. */
const int FILE_PUSH_INTERVAL = 5000;
/* The number of milliseconds to wait between rounds of checking the file server
 * and workers. */
const int CHECKUP_INTERVAL = 5000;

/* Information about a worker. */
typedef struct WorkerInfo {
    /* Construct a WorkerInfo. */
    WorkerInfo(std::string addr) {
      this->addr = addr;
    }

    std::string addr; // The worker's address (hostname and port).
} WorkerInfo;

std::vector<double> model_state;
std::vector<double> old_state;
const double LEARN_RATE = 0.5;

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


/* A stub for communicating with a file server via its API (gRPC service). */
class FileServerStub {
 public:
  /* Construct a FileServerStub. */
  FileServerStub(std::shared_ptr<Channel> channel)
      : stub_(FileServer::NewStub(channel)) {

  }

  /* Tell the file server to do a push. */
  void DoPush(std::string recipient_addr, int file_num) {
    std::cout << "requesting push" << std::endl;
    Push push;
    PushOutcome outcome;
    ClientContext context;
    push.set_recipient_addr(recipient_addr);
    push.set_file_num(file_num);
    Status status = stub_->DoPush(&context, push, &outcome);
    if (status.ok() && outcome.ok()) {
      std::cout << "push request succeeded" << std::endl;
    } else {
      std::cout << "push request failed" << std::endl;
    }
  }

  /* Check to see if the FileServer is still alive, and
   * respond to any load feedback sent. */
  void CheckUp() {
    std::cout << "checking on FileServer" << std::endl;
    Empty empty;
    LoadFeedback feedback;
    ClientContext context;
    Status status = stub_->CheckUp(&context, empty, &feedback);
    if (status.ok()) {
      // TODO:  respond to LoadFeedback
      std::cout << "checkup on FileServer succeeded" << std::endl;
    } else {
      std::cout << "checkup on FileServer failed" << std::endl;
    }
  }

 private:
  std::unique_ptr<FileServer::Stub> stub_;
};


/* A stub for communicating with a worker via its API (gRPC service). */
class WorkerStub {
 public:
  /* Construct a WorkerStub. */
  WorkerStub(std::shared_ptr<Channel> channel, int id)
      : stub_(Worker::NewStub(channel)), id(id) {

  }

  /* Tell the file server to do a push. */
  void CheckUp() {
    std::cout << "checking on worker" << id << std::endl;
    PeerList peer_list;
    FlowFeedback feedback;
    ClientContext context;

    // copy in worker addrs to the peer list
    workers_mutex.lock();
    for (std::shared_ptr<WorkerInfo> worker_info : workers) {
      peer_list.add_peer_addrs(worker_info->addr);
    }
    workers_mutex.unlock();

    Status status = stub_->CheckUp(&context, peer_list, &feedback);
    if (status.ok()) {
      std::cout << "checkup on worker" << id << " succeeded" << std::endl;
    } else {
      std::cout << "checkup on worker" << id << " failed" << std::endl;
    }
  }

 private:
  std::unique_ptr<Worker::Stub> stub_;
  int id;
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

/* Periodically request file pushes to workers.
 *
 * Currently requests a push of file 0 to each worker.  */
void periodically_request_pushes() {
  FileServerStub file_server(
      grpc::CreateChannel(FILE_SERVER_ADDR,
                          grpc::InsecureChannelCredentials()));
  while (true) {
    workers_mutex.lock();
    std::vector<std::string> worker_addrs;
    for (std::shared_ptr<WorkerInfo> worker_info : workers) {
      worker_addrs.push_back(worker_info->addr);
    }
    workers_mutex.unlock();
    for (std::string addr : worker_addrs) {

      file_server.DoPush(addr, 0);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(FILE_PUSH_INTERVAL));
  }
}


void periodically_do_checkups() {
  FileServerStub file_server(
      grpc::CreateChannel(FILE_SERVER_ADDR,
                          grpc::InsecureChannelCredentials()));

  while (true) {
    file_server.CheckUp();

    workers_mutex.lock();
    std::vector<std::string> worker_addrs;
    for (std::shared_ptr<WorkerInfo> worker_info : workers) {
      worker_addrs.push_back(worker_info->addr);
    }
    workers_mutex.unlock();

    int i = 0;
    for (std::string addr : worker_addrs) {
      // TODO (PERF):  don't reconstruct stubs every time!
      auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
      WorkerStub worker(channel, i);
      worker.CheckUp();
      i++;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(CHECKUP_INTERVAL));
  }
}

void periodically_send_updates() {

  while (true) {
    

    workers_mutex.lock();
    int lucky_worker_index = rand() % workers.size();
    std::string lucky_worker_addr = workers[lucky_worker_index].addr;
    workers_mutex.unlock();


    Update update;
    for (int i = 0; i < model_state.size();i++){
      update->add_delta(model_state[i] - old_state[i]);  
    }

    auto channel = grpc::CreateChannel(lucky_worker_addr, grpc::InsecureChannelCredentials());
    WorkerStub worker(channel, lucky_worker_index);
    worker.ExchangeUpdates(&update);
    // i++;
    // }
    old_state = model_state;

    std::this_thread::sleep_for(std::chrono::milliseconds(GOSSIP_INTERVAL));
  }
}

int main(int argc, char** argv) {
  // Run the server in another thread.
  std::thread server_thread(serve_requests);
  // Create daemon to check if file server and workers are still alive
  std::thread checkup_thread(periodically_do_checkups);

  // Periodically push files.
  periodically_request_pushes();

  // Wait for the server.
  server_thread.join();
  // Wait for the daemon.
  checkup_thread.join();

  return 0;
}
