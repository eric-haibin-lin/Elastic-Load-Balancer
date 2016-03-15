#include <glog/logging.h>
#include <stdio.h>
#include <stdlib.h>

#include "server/messages.h"
#include "server/master.h"
#include <map>
#include <queue> 

static struct Master_state {

  // The mstate struct collects all the master node state into one
  // place.  You do not need to preserve any of the fields below, they
  // exist only to implement the basic functionality of the starter
  // code.

  bool server_ready;
  int max_num_workers;
  int num_ongoing_client_requests;
  int next_tag;

  Worker_handle my_worker;
  std::queue<Request_msg> message_queue;
  std::map<int, Client_handle> tag_client_map;

} mstate;


void master_node_init(int max_workers, int& tick_period) {

  // set up tick handler to fire every 5 seconds. (feel free to
  // configure as you please)
  tick_period = 5;

  mstate.next_tag = 0;
  mstate.max_num_workers = max_workers;
  mstate.num_ongoing_client_requests = 0;

  // don't mark the server as ready until the server is ready to go.
  // This is actually when the first worker is up and running, not
  // when 'master_node_init' returnes
  mstate.server_ready = false;

  // fire off a request for a new worker

  int tag = random();
  Request_msg req(tag);
  req.set_arg("name", "my worker 0");
  request_new_worker_node(req);

}

void handle_new_worker_online(Worker_handle worker_handle, int tag) {

  // 'tag' allows you to identify which worker request this response
  // corresponds to.  Since the starter code only sends off one new
  // worker request, we don't use it here.
  mstate.my_worker = worker_handle;

  // Now that a worker is booted, let the system know the server is
  // ready to begin handling client requests.  The test harness will
  // now start its timers and start hitting your server with requests.
  if (mstate.server_ready == false) {
    server_init_complete();
    mstate.server_ready = true;
  }
}

void handle_worker_response(Worker_handle worker_handle, const Response_msg& resp) {

  // Master node has received a response from one of its workers.
  // Here we directly return this response to the client.
  auto tag = resp.get_tag();
  DLOG(INFO) << "Master received a response from a worker: [" << tag << ":" << resp.get_response() << "]" << std::endl;

  send_client_response(mstate.tag_client_map[tag], resp);
  mstate.num_ongoing_client_requests--;

  // Send the next message if necessary
  if (mstate.message_queue.size() > 0) {
    // Get the next request 
    auto request = mstate.message_queue.front();
    mstate.message_queue.pop();

    // You can assume that traces end with this special message.  It
    // exists because it might be useful for debugging to dump
    // information about the entire run here: statistics, etc.
    if (request.get_arg("cmd") == "lastrequest") {
      Response_msg resp(0);
      resp.set_response("ack");
      auto client = mstate.tag_client_map[request.get_tag()];
      send_client_response(client, resp);
      return;
    } else {
      send_request_to_worker(mstate.my_worker, request);  
      mstate.num_ongoing_client_requests++;
    }
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
  mstate.tag_client_map[tag] = client_handle;
  
  // The provided starter code cannot handle multiple pending client
  // requests.  The server returns an error message, and the checker
  // will mark the response as "incorrect"
  if (mstate.num_ongoing_client_requests > 0) {
    mstate.message_queue.push(worker_req);
  } else {
    send_request_to_worker(mstate.my_worker, worker_req);
    mstate.num_ongoing_client_requests++;
  }

  // We're done!  This event handler now returns, and the master
  // process calls another one of your handlers when action is
  // required.

}


void handle_tick() {

  // TODO: you may wish to take action here.  This method is called at
  // fixed time intervals, according to how you set 'tick_period' in
  // 'master_node_init'.

}

