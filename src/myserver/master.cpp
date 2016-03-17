#include <glog/logging.h>
#include <stdio.h>
#include <stdlib.h>

#include "server/messages.h"
#include "server/master.h"
#include <map>
#include <queue> 
#include <string>

struct Count_prime_result {
  int count;
  int first_tag;
  int data;
};

static struct Master_state {

  // The mstate struct collects all the master node state into one
  // place.  You do not need to preserve any of the fields below, they
  // exist only to implement the basic functionality of the starter
  // code.

  bool server_ready;
  int max_num_workers;
  int num_ongoing_client_requests;
  int next_tag;

  Worker_handle my_worker[16];
  int worker_idx;
  std::queue<Request_msg> message_queue;
  std::map<int, Client_handle> client_map;
  std::map<int, Count_prime_result> prime_map;

} mstate;

void send(Request_msg worker_req);

void master_node_init(int max_workers, int& tick_period) {

  // set up tick handler to fire every 5 seconds. (feel free to
  // configure as you please)
  tick_period = 5;

  mstate.next_tag = 0;
  mstate.max_num_workers = max_workers;
  mstate.worker_idx = 0;
  mstate.num_ongoing_client_requests = 0;

  // don't mark the server as ready until the server is ready to go.
  // This is actually when the first worker is up and running, not
  // when 'master_node_init' returnes
  mstate.server_ready = false;

  for (int i = 0; i < max_workers; i++) {
    // fire off a request for a new worker
    Request_msg req(i);
    req.set_arg("name", "my worker " + std::to_string(i));
    request_new_worker_node(req);
  }
}

void handle_new_worker_online(Worker_handle worker_handle, int tag) {

  // 'tag' allows you to identify which worker request this response
  // corresponds to.  Since the starter code only sends off one new
  // worker request, we don't use it here.
  mstate.my_worker[mstate.worker_idx++] = worker_handle;

  // Now that a worker is booted, let the system know the server is
  // ready to begin handling client requests.  The test harness will
  // now start its timers and start hitting your server with requests.
  if (mstate.server_ready == false && mstate.worker_idx == mstate.max_num_workers) {
    mstate.worker_idx = 0;
    server_init_complete();
    mstate.server_ready = true;
  }
}

void handle_worker_response(Worker_handle worker_handle, const Response_msg& resp) {

  // Master node has received a response from one of its workers.
  // Here we directly return this response to the client.
  auto tag = resp.get_tag();
  DLOG(INFO) << "Master received a response from a worker: [" << tag << ":" << resp.get_response() << "]" << std::endl;

  // aggregate results for compare_prime
  if (mstate.prime_map.find(tag) != mstate.prime_map.end()) {
    auto& current = mstate.prime_map[tag];
    auto first_tag = current.first_tag;
    auto& first = mstate.prime_map[first_tag];
    // increment counter
    first.count++;
    // update data
    current.data = std::stoi(resp.get_response());
    if (first.count == 4) {
      // the other results
      auto& second = mstate.prime_map[first_tag + 1];
      auto& third = mstate.prime_map[first_tag + 2];
      auto& fourth = mstate.prime_map[first_tag + 3];

      // compare 
      Response_msg aggregate_resp(0);
      if (second.data - first.data > fourth.data - third.data) {
        aggregate_resp.set_response("There are more primes in first range.");
      } else {
        aggregate_resp.set_response("There are more primes in second range.");
      }
      // send result
      send_client_response(mstate.client_map[first_tag], aggregate_resp);
      mstate.client_map.erase(first_tag);
    }
    mstate.num_ongoing_client_requests--;

  } else {
    // not compare_prime: just send the result back
    send_client_response(mstate.client_map[tag], resp);
    mstate.client_map.erase(tag);
    mstate.num_ongoing_client_requests--;
  }

  // Send the ack message if necessary
  if (mstate.num_ongoing_client_requests == 0 && mstate.message_queue.size() > 0) {
    // Get the ack request
    auto request = mstate.message_queue.front();
    mstate.message_queue.pop();

    // You can assume that traces end with this special message.  It
    // exists because it might be useful for debugging to dump
    // information about the entire run here: statistics, etc.
    Response_msg resp(0);
    resp.set_response("ack");
    auto client = mstate.client_map[request.get_tag()];
    send_client_response(client, resp);
    return;
  }
}

void handle_client_request(Client_handle client_handle, const Request_msg& client_req) {

  DLOG(INFO) << "Received request: " << client_req.get_request_string() << std::endl;

  // Fire off the request to the worker.  Eventually the worker will
  // respond, and your 'handle_worker_response' event handler will be
  // called to forward the worker's response back to the server.
  int tag = mstate.next_tag++;
  Request_msg worker_req(tag, client_req);

  // Save off the handle to the client that is expecting a response.
  // The master needs to do this it can response to this client later
  // when 'handle_worker_response' is called.
  mstate.client_map[tag] = client_handle;
  
  if (client_req.get_arg("cmd") == "lastrequest") {
      mstate.message_queue.push(worker_req);
      return;
  }

  if (client_req.get_arg("cmd") == "compareprimes") {
    auto first_tag = tag;
    // init the struct for partial results
    struct Count_prime_result results;
    results.count = 0;
    results.first_tag = first_tag;
    mstate.prime_map[tag] = results;
    
    // send the first request, which computes n1
    worker_req.set_arg("param", "n1"); 
    send(worker_req);

    // the other three requests, which computes n2, n3, n4
    for (int i = 1; i < 4; i++) {
      // init the struct for partial results
      struct Count_prime_result results;
      results.count = 0;
      results.first_tag = first_tag;
      mstate.prime_map[mstate.next_tag] = results;

      // param: n2, n3, n4
      worker_req.set_tag(mstate.next_tag++);
      worker_req.set_arg("param", "n" + std::to_string(i + 1));
      send(worker_req);
    }
  } else {
    send(worker_req);
  }

  // We're done!  This event handler now returns, and the master
  // process calls another one of your handlers when action is
  // required.
}

void send(Request_msg worker_req) {
  // Round robin all workers
  send_request_to_worker(mstate.my_worker[mstate.worker_idx], worker_req);
  mstate.num_ongoing_client_requests++;
  mstate.worker_idx = (mstate.worker_idx + 1) % mstate.max_num_workers;  
}

void handle_tick() {

  // TODO: you may wish to take action here.  This method is called at
  // fixed time intervals, according to how you set 'tick_period' in
  // 'master_node_init'.

}

