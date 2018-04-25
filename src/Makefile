# Adapted from an example at
# 	https://github.com/grpc/grpc/tree/v1.11.x/examples/cpp/route_guide.

HOST_SYSTEM = $(shell uname | cut -f 1 -d_)
SYSTEM ?= $(HOST_SYSTEM)
CXX = g++
CPPFLAGS += `pkg-config --cflags protobuf grpc`
CXXFLAGS += -std=c++11
ifeq ($(SYSTEM),Darwin)
LDFLAGS += -L/usr/local/lib `pkg-config --libs protobuf grpc++`\
           -lgrpc++_reflection\
           -ldl
else
LDFLAGS += -L/usr/local/lib `pkg-config --libs protobuf grpc++`\
           -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed\
           -ldl
endif
PROTOC = protoc
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

PROTOS_PATH = protos

vpath %.proto $(PROTOS_PATH)

all: worker master file_server

worker: serverless_learn.pb.o serverless_learn.grpc.pb.o worker.o
	$(CXX) $^ $(LDFLAGS) -o $@

master: serverless_learn.pb.o serverless_learn.grpc.pb.o master.o
	$(CXX) $^ $(LDFLAGS) -o $@

file_server: serverless_learn.pb.o serverless_learn.grpc.pb.o file_server.o
	$(CXX) $^ $(LDFLAGS) -o $@

%.grpc.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --grpc_out=. --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<

%.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=. $<

clean:
	rm -f *.o *.pb.cc *.pb.h worker master file_server
