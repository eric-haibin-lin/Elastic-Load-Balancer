
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sstream>
#include <glog/logging.h>

#include "server/messages.h"
#include "server/worker.h"
#include "tools/cycle_timer.h"
#include <thread>
#include <string>
#include "tools/work_queue.h"

// TODO Maybe 23 threads is more appropriate
#define NUM_THREADS 44
#define TELL_ME_NOW_NUM_THREADS 1
#define PROJ_IDEA_NUM_THREADS 2 

static WorkQueue<Request_msg> queue;
static WorkQueue<Request_msg> tell_me_now_queue;
static WorkQueue<Request_msg> proj_idea_queue;

static std::thread threads[NUM_THREADS];
static std::thread tell_me_now_threads[TELL_ME_NOW_NUM_THREADS];
static std::thread proj_idea_threads[PROJ_IDEA_NUM_THREADS];

void worker_thread();
void tell_me_now_worker_thread();
void proj_idea_worker_thread();

// Generate a valid 'countprimes' request dictionary from integer 'n'
static void create_computeprimes_req(Request_msg& req, int n) {
  std::ostringstream oss;
  oss << n;
  req.set_arg("cmd", "countprimes");
  req.set_arg("n", oss.str());
}

// Implements logic required by compareprimes command via multiple
// calls to execute_work.  This function fills in the appropriate
// response.
static void execute_compareprimes(const Request_msg& req, Response_msg& resp) {

    int params[4];
    int counts[4];

    // grab the four arguments defining the two ranges
    params[0] = atoi(req.get_arg("n1").c_str());
    params[1] = atoi(req.get_arg("n2").c_str());
    params[2] = atoi(req.get_arg("n3").c_str());
    params[3] = atoi(req.get_arg("n4").c_str());

    for (int i=0; i<4; i++) {
      Request_msg dummy_req(0);
      Response_msg dummy_resp(0);
      create_computeprimes_req(dummy_req, params[i]);
      execute_work(dummy_req, dummy_resp);
      counts[i] = atoi(dummy_resp.get_response().c_str());
    }

    if (counts[1]-counts[0] > counts[3]-counts[2])
      resp.set_response("There are more primes in first range.");
    else
      resp.set_response("There are more primes in second range.");
}

// execute parts of compareprimes
static void execute_compareprimes_partial(const Request_msg& req, Response_msg& resp) {

  auto params = req.get_arg("param");
  Request_msg dummy_req(0);
  Response_msg dummy_resp(0);
  create_computeprimes_req(dummy_req, atoi(req.get_arg(params).c_str()));
  execute_work(dummy_req, dummy_resp);
  resp.set_response(dummy_resp.get_response().c_str());
}


void worker_node_init(const Request_msg& params) {

  // This is your chance to initialize your worker.  For example, you
  // might initialize a few data structures, or maybe even spawn a few
  // pthreads here.  Remember, when running on Amazon servers, worker
  // processes will run on an instance with a dual-core CPU.

  DLOG(INFO) << "**** Initializing worker: " << params.get_arg("name") << " ****\n";

  for (int i = 0; i < NUM_THREADS; i++) {
    threads[i] = std::thread(worker_thread);
    threads[i].detach();
  }
  for (int i = 0; i < TELL_ME_NOW_NUM_THREADS; i++) {
    tell_me_now_threads[i] = std::thread(tell_me_now_worker_thread);
    tell_me_now_threads[i].detach();
  }
  for (int i = 0; i < PROJ_IDEA_NUM_THREADS; i++) {
    proj_idea_threads[i] = std::thread(proj_idea_worker_thread);
    proj_idea_threads[i].detach();
  }
}

void proj_idea_worker_thread() {
  while (true) {
    auto req = proj_idea_queue.get_work();
    Response_msg resp(req.get_tag());
    DLOG(INFO) << "Worker got request: [" << req.get_tag() << ":" << req.get_request_string() << "]\n";

    double startTime = CycleTimer::currentSeconds();
    execute_work(req, resp);

    double dt = CycleTimer::currentSeconds() - startTime;
    DLOG(INFO) << "Worker completed work in " << (1000.f * dt) << " ms (" << req.get_tag()  << ")\n";

    // send a response string to the master
    worker_send_response(resp);
  }
}

void tell_me_now_worker_thread() {
  while (true) {
    auto req = tell_me_now_queue.get_work();
    Response_msg resp(req.get_tag());
    DLOG(INFO) << "Worker got request: [" << req.get_tag() << ":" << req.get_request_string() << "]\n";

    double startTime = CycleTimer::currentSeconds();
    execute_work(req, resp);

    double dt = CycleTimer::currentSeconds() - startTime;
    DLOG(INFO) << "Worker completed work in " << (1000.f * dt) << " ms (" << req.get_tag()  << ")\n";

    // send a response string to the master
    worker_send_response(resp);
  }
}

void worker_thread() {
  while (true) {

    auto req = queue.get_work();
    // Make the tag of the reponse match the tag of the request.  This
    // is a way for your master to match worker responses to requests.
    Response_msg resp(req.get_tag());

    // Output debugging help to the logs (in a single worker node
    // configuration, this would be in the log logs/worker.INFO)
    DLOG(INFO) << "Worker got request: [" << req.get_tag() << ":" << req.get_request_string() << "]\n";

    double startTime = CycleTimer::currentSeconds();

    if (req.get_arg("cmd").compare("compareprimes") == 0) {

      // The compareprimes command needs to be special cased since it is
      // built on four calls to execute_execute work.  All other
      // requests from the client are one-to-one with calls to
      // execute_work.
      execute_compareprimes_partial(req, resp);

    } else {

      // actually perform the work.  The response string is filled in by
      // 'execute_work'
      execute_work(req, resp);

    }

    double dt = CycleTimer::currentSeconds() - startTime;
    DLOG(INFO) << "Worker completed work in " << (1000.f * dt) << " ms (" << req.get_tag()  << ")\n";

    // send a response string to the master
    worker_send_response(resp);
  }
}

void worker_handle_request(const Request_msg& req) {

  // case tellmenow: do it instantly 
  if (req.get_arg("cmd") == "tellmenow") {
    tell_me_now_queue.put_work(req);  
  } else if (req.get_arg("cmd") == "projectidea") {
    proj_idea_queue.put_work(req);
  } else {
    queue.put_work(req);  
  }

}
