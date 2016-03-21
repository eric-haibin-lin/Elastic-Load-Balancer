#include <glog/logging.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "server/messages.h"
#include "server/master.h"
#include "server/worker.h"
#include "tools/cycle_timer.h"
#include <map>
#include <string>

#define TICK_PERIOD 1
#define PROJECT_IDEA_THRESHOLD 4000
#define OTHER_THRESHOLD 2400
#define SLOWER_UB 0.2
#define SLOWER_LB 0.02
#define REQ_WKR_THD 24


struct Count_prime_result {
    int count;
    int first_tag;
    int data;
};

struct Worker_count {
    bool if_idle;
    bool if_shutdown;
    int idx;
    int ongoing_req_count;
    int project_idea_count;
};

static struct Master_state {

    // The mstate struct collects all the master node state into one
    // place.  You do not need to preserve any of the fields below, they
    // exist only to implement the basic functionality of the starter
    // code.

    bool server_ready;
    int max_num_workers;
    int num_ongoing_client_requests;
    int last_ongoing_req_num;
    int num_slower;
    int next_tag;
    int num_worker;

    Worker_handle my_worker[4];
    Worker_count workers[4];
    std::map<int, Client_handle> client_map;
    std::map<int, Count_prime_result> compare_prime_map;
    std::map<std::string, Response_msg> count_prime_map;
    std::map<int, std::string> request_map;
    std::map<int, int> worker_assignment_map;
    std::map<int, int> project_idea_map;
    std::map<int, int> new_worker_map;
    std::map<int, double> tag_time_map;
    //TODO track the workload of each worker node


} mstate;

void send(Request_msg worker_req);

void master_node_init(int max_workers, int& tick_period) {

    // set up tick handler to fire every 5 seconds. (feel free to
    // configure as you please)
    tick_period = TICK_PERIOD;

    mstate.next_tag = 0;
    mstate.max_num_workers = max_workers;
    mstate.num_ongoing_client_requests = 0;
    mstate.last_ongoing_req_num = 0;
    mstate.num_slower = 0;
    mstate.num_worker = 0;
    for(int i = 0; i < 4; ++i){
        mstate.workers[i].if_idle = true;
        mstate.workers[i].if_shutdown = true;
    }
    // don't mark the server as ready until the server is ready to go.
    // This is actually when the first worker is up and running, not
    // when 'master_node_init' returnes
    mstate.server_ready = false;

    // fire off a request for a new worker
    Request_msg req(0);
    req.set_arg("name", "my worker " + std::to_string(0));
    request_new_worker_node(req);
    mstate.new_worker_map[0] = 0;
}

void worker_count_init(Worker_count * wc, int idx){
    wc->if_idle = false;
    wc->if_shutdown = false;
    wc->project_idea_count = 0;
    wc->idx = idx;
    wc->ongoing_req_count = 0;
}

void handle_new_worker_online(Worker_handle worker_handle, int tag) {

    // 'tag' allows you to identify which worker request this response
    // corresponds to.  Since the starter code only sends off one new
    // worker request, we don't use it here.

    int idx = mstate.new_worker_map[tag];
    mstate.new_worker_map.erase(tag);
    mstate.my_worker[idx] = worker_handle;
    worker_count_init(&mstate.workers[idx], idx);

    mstate.num_worker++;

    // Now that a worker is booted, let the system know the server is
    // ready to begin handling client requests.  The test harness will
    // now start its timers and start hitting your server with requests.
    if (mstate.server_ready == false) {
        server_init_complete();
        mstate.server_ready = true;
    }
}

// TODO: update the workload of the worker node as well 

void handle_worker_response(Worker_handle worker_handle, const Response_msg& resp) {

    // Master node has received a response from one of its workers.
    // Here we directly return this response to the client.
    auto tag = resp.get_tag();

    mstate.num_ongoing_client_requests--;

    auto idx = mstate.worker_assignment_map[tag];
    mstate.workers[idx].ongoing_req_count--;
    mstate.worker_assignment_map.erase(tag);

    // count time
    double startTime = mstate.tag_time_map[tag];
    mstate.tag_time_map.erase(tag);

    double latency = 1000.f * (CycleTimer::currentSeconds() - startTime);

    DLOG(INFO) << "Master received a response from a worker: [" << tag << ":" << resp.get_response() << "], with latency: " << latency << "ms." << std::endl;


    // special case: project_idea
    if (mstate.project_idea_map.find(tag) != mstate.project_idea_map.end()){
        auto idx = mstate.project_idea_map[tag];
        mstate.workers[idx].project_idea_count--;
        mstate.project_idea_map.erase(tag);
        if(latency > PROJECT_IDEA_THRESHOLD){
            mstate.num_slower++;
        }
    } else {
        if(latency > OTHER_THRESHOLD){
            mstate.num_slower++;
        }
    }

    // special case: compute_prime. cache the result
    if (mstate.request_map.find(tag) != mstate.request_map.end()) {
        auto request_string = mstate.request_map[tag];
        mstate.count_prime_map[request_string] = resp;
        mstate.request_map.erase(tag);
    }


    // special case: compare_prime, aggregate results 
    if (mstate.compare_prime_map.find(tag) != mstate.compare_prime_map.end()) {
        auto& current = mstate.compare_prime_map[tag];
        auto first_tag = current.first_tag;
        auto& first = mstate.compare_prime_map[first_tag];
        // increment counter
        first.count++;
        // update data
        current.data = std::stoi(resp.get_response());
        if (first.count == 4) {
            // the other results
            auto& second = mstate.compare_prime_map[first_tag + 1];
            auto& third = mstate.compare_prime_map[first_tag + 2];
            auto& fourth = mstate.compare_prime_map[first_tag + 3];

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
    } else {
        // otherwise, just send the result back
        send_client_response(mstate.client_map[tag], resp);
        mstate.client_map.erase(tag);
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

    // lastrequest
    if (client_req.get_arg("cmd") == "lastrequest") {
        Response_msg resp(mstate.next_tag);
        resp.set_response("ack");
        send_client_response(client_handle, resp);
        return;
    }

    // compareprimes
    if (client_req.get_arg("cmd") == "compareprimes") {
        auto first_tag = tag;
        // init the struct for partial results
        struct Count_prime_result results;
        results.count = 0;
        results.first_tag = first_tag;
        mstate.compare_prime_map[tag] = results;

        // send the first request, which computes n1
        worker_req.set_arg("param", "n1"); 
        send(worker_req);

        // the other three requests, which computes n2, n3, n4
        for (int i = 1; i < 4; i++) {
            // init the struct for partial results
            struct Count_prime_result results;
            results.count = 0;
            results.first_tag = first_tag;
            mstate.compare_prime_map[mstate.next_tag] = results;

            // set param: n2, n3, n4
            worker_req.set_tag(mstate.next_tag++);
            worker_req.set_arg("param", "n" + std::to_string(i + 1));
            send(worker_req);
        }
        return;
    } 

    // countprimes
    if (client_req.get_arg("cmd") == "countprimes") {
        auto request_string = client_req.get_request_string();
        // repeated request - return directly
        if (mstate.count_prime_map.find(request_string) != mstate.count_prime_map.end()) {
            send_client_response(client_handle, mstate.count_prime_map[request_string]);
            mstate.client_map.erase(tag);
        } else {
            // request not cache'd yet 
            mstate.request_map[tag] = request_string;
            send(worker_req);
        }
        return;
    }

    // otherwise
    send(worker_req);

    // We're done!  This event handler now returns, and the master
    // process calls another one of your handlers when action is
    // required.
}

void send(Request_msg worker_req) {

    // TODO: we want to check the workload of each workers before the
    // task assignment, esp. projectidea tasks
    // TODO: have special case for tell_me_now;
    int idx = 0;

    if (worker_req.get_arg("cmd") == "projectidea") {
        int min = 999999;
        for (int i = 0; i < mstate.max_num_workers; i++) {
            if (!mstate.workers[i].if_shutdown && !mstate.workers[i].if_idle && mstate.workers[i].project_idea_count < min) {
                min = mstate.workers[i].project_idea_count;
                idx = i;
            }
        }
        mstate.workers[idx].project_idea_count++;
        mstate.project_idea_map[worker_req.get_tag()] = idx;
    } else {
        int min = 999999;
        for(int i = 0; i < mstate.max_num_workers; ++i){
            if(!mstate.workers[i].if_shutdown && !mstate.workers[i].if_idle && mstate.workers[i].ongoing_req_count < min) {
                min = mstate.workers[i].ongoing_req_count;
                idx = i;
            }
        }
    } 


    send_request_to_worker(mstate.my_worker[idx], worker_req);
    mstate.workers[idx].ongoing_req_count++;
    mstate.num_ongoing_client_requests++;
    mstate.last_ongoing_req_num++;
    mstate.worker_assignment_map[worker_req.get_tag()] = idx;
    double startTime = CycleTimer::currentSeconds();
    mstate.tag_time_map[worker_req.get_tag()] = startTime;

    DLOG(INFO) << "Sending to worker: " << idx << std::endl;

    return;
}

void handle_tick() {

    // TODO: you may wish to take action here.  This method is called at
    // fixed time intervals, according to how you set 'tick_period' in
    // 'master_node_init'.

    // TODO: periodically check the load of each worker node, and shutdown/
    // boot up new worker nodes if necessary. Currently the number of 
    // workers is fixed

    bool if_project_idea_exceeds = false;
    // step 1: collect garbage worker node;
    for(int i = 0; i < mstate.max_num_workers; ++i){
        if(mstate.workers[i].if_shutdown && !mstate.workers[i].if_idle && mstate.workers[i].ongoing_req_count == 0){
            //need to collect
            kill_worker_node(mstate.my_worker[i]);  
            mstate.workers[i].if_idle = true;
        }
        if(mstate.workers[i].project_idea_count > 2)
            if_project_idea_exceeds = true;
    }



    // If too much requests, then let's have a new worker node
    double slower_ratio = 0;
    if(mstate.last_ongoing_req_num != 0){
        slower_ratio = (double)mstate.num_slower / (double)mstate.last_ongoing_req_num;
    }

    while(mstate.num_worker < mstate.max_num_workers && (slower_ratio > SLOWER_UB || if_project_idea_exceeds)) {
        // open a new node;
        DLOG(INFO) << "Needs add a worker_node" << CycleTimer::currentSeconds() << std::endl;
        DLOG(INFO) << "Num_worker: " << mstate.num_worker << std::endl;
        DLOG(INFO) << "SLOWER_Ratio: " << slower_ratio << std::endl; 
        for(int i = 0 ; i < mstate.max_num_workers; ++i){
            if(mstate.workers[i].if_shutdown && !mstate.workers[i].if_idle){
                //reopen this;
                mstate.workers[i].if_shutdown = false;
                mstate.num_worker++;
                DLOG(INFO) << "Adding worker: " << i << std::endl;
                break;
            }
            if(mstate.workers[i].if_idle){
                Request_msg req(mstate.next_tag);
                req.set_arg("name", "my worker " + std::to_string(i));
                request_new_worker_node(req);
                mstate.new_worker_map[mstate.next_tag] = i;
                mstate.next_tag++;
                DLOG(INFO) << "Adding worker: " << i << std::endl;
                break;
            }
        }
        mstate.num_slower = 0;
        mstate.last_ongoing_req_num = 0;
        return;
    } 

    if(mstate.num_worker > 1 && slower_ratio < SLOWER_LB  && ((double)mstate.num_ongoing_client_requests / (double) (mstate.num_worker - 1) < REQ_WKR_THD )) {
        DLOG(INFO) << "Needs kill a worker_node" << CycleTimer::currentSeconds() << std::endl;
        DLOG(INFO) << "Num_worker: " << mstate.num_worker << std::endl;
        DLOG(INFO) << "SLOWER_Ratio: " << slower_ratio << std::endl; 
        DLOG(INFO) << "REQ_WKR_RATIO: " <<(double)mstate.num_ongoing_client_requests / (double) (mstate.num_worker - 1) << std::endl; 
        int min = 99999;
        int idx = -1;
        for(int i = 0 ; i < mstate.max_num_workers; ++i){
            if(!mstate.workers[i].if_shutdown && mstate.workers[i].ongoing_req_count < min && mstate.workers[i].project_idea_count == 0){
                min = mstate.workers[i].ongoing_req_count;
                idx = i;
            }
        }  
        if(idx >= 0){
            //decide to terminate this one;
            DLOG(INFO) << "Killing worker: " << idx << std::endl;
            mstate.workers[idx].if_shutdown = true;
            mstate.num_worker--;
        }
        mstate.num_slower = 0;
        mstate.last_ongoing_req_num = 0;
    }

}

