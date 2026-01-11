// #include <bits/stdc++.h>
#include <iostream>
#include <string>
#include <map>
#include <set>
#include <vector>
#include <condition_variable>
#include <grpcpp/grpcpp.h>
#include "rpc_messages/msg.pb.h"
#include "rpc_messages/msg.grpc.pb.h"
#include <thread>
#include <mutex>
#include <cmath>
#include <unistd.h>
#include <queue>
#include <sys/wait.h>
#include <iomanip>
#include <fstream>
#include <sstream>
#include <random>
#include <chrono>
#include <ctime>
#include <sys/types.h>
#include <sys/stat.h>
#include <filesystem>
#include <algorithm>
#include <optional>
#include <memory>
#include <limits>
#include <cctype>
#include <unordered_map>
#include <unordered_set>
#include <cryptopp/rsa.h>
#include <cryptopp/osrng.h>
#include <cryptopp/sha.h>
#include <cryptopp/files.h>
#include <cryptopp/filters.h>
#include <cryptopp/hex.h>

using namespace std;
using namespace grpc;
using namespace msg;

struct Log
{
    pair<int,int> b; // (Ballot number, node_id)
    int seq_num; // Sequence number
    pair<pair<string,string>,int> tns;  // Transaction
    int unique; // Unique number of client request

    Log(const pair<int,int> &b, int seq_num,const pair<pair<string,string>,int> &tns, int unique){
        this->b=move(b);
        this->seq_num=move(seq_num);
        this->tns=move(tns);
        this->unique=move(unique);
    }

    Log(){
        this->b={-1,-1};
        this->seq_num=-1;
        this->tns={{"__","__"},-1};
        this->unique=-1;
    }

};

struct Client_Req
{
    pair<pair<string,string>,int> tns;  // Transaction
    int client_id;
    int unique;

    Client_Req(const pair<pair<string,string>,int> &tns, int client_id, int unique){
        this->tns=move(tns);
        this->client_id=move(client_id);
        this->unique=move(unique);
    }
};

enum class BatchOpType {
    Transfer,
    Read,
    LF
};

struct BatchOperation {
    BatchOpType type = BatchOpType::Transfer;
    std::string from;
    std::string to;
    int amount = 0;
};

struct ReadConsensusResult {
    bool consensus_reached = false;
    bool value_found = false;
    int amount = 0;
    std::vector<ClientReadReplyEntry> supporting_entries;
};

class Node : public msg::NodeService::Service{

    public:
        msg::NodeService::AsyncService* service_;  // pointer to async service
        std::unique_ptr<ServerCompletionQueue> cq_;
        std::unique_ptr<Server> server_;
        std::thread async_server_thread_;

        int node_id;
        string  address;
        map<int, shared_ptr<msg::NodeService::Stub>> stubs; // peer_id -> address
        map<int, shared_ptr<msg::NodeService::Stub>> client_stubs; // peer_id -> address
        map<string,int> db; // Simple key-value store as database
        set<int> commit_set;
        set<int>dark_set;
        set<int>equi_set;
        bool is_leader;
        bool timer_stopped;
        bool reset_flag;
        bool election_completed;
        bool trigger_tns;
        bool stop_vc_timer;
        bool block_msg_vc;
        int ballot_num;
        int cur_leader;
        int seq_num;
        int votes;
        int nodes_num;
        int execution_index;
        bool running_election;
        bool stop_process;
        bool crash;
        bool sign;
        bool dark;
        bool equivocation;
        bool time_delay;
        bool reset_vc_timer;
        thread node_server;
        thread node_async_server;
        thread node_timer;
        vector<Log> logs;
        queue<Client_Req> client_reqs;
        mutex main_process;
        mutex m;
        mutex time_mutex;
        mutex stopped_mutex;
        mutex logs_mutex;
        mutex reset_flag_mutex;
        mutex stop_process_mutex;
        mutex election;
        mutex new_view_mutex;
    mutex view_log_file_mutex;
        mutex seq_num_mutex;
        mutex commit_set_mutex;
        mutex processing_msg;
        mutex temp_mutex;
        mutex vote_mutex;
        mutex form_new_log_view_mutex;
        mutex time_vc_mutex;
        condition_variable cv;
        condition_variable cv_election;
        condition_variable election_cv;
        condition_variable cv_stop_process;
        condition_variable cv_main_process;
        condition_variable cv_log_entry_or_heatbeat;
        condition_variable cv_vote;
        condition_variable cv_vc_timer;
        chrono::milliseconds timeout;
        chrono::milliseconds heartbeat_timer;

        int random_timeout;

        chrono::milliseconds process_stop_timer;
        thread timerThread;
        vector<thread> election_threads;
        map<int,int> election_map; //ballot_num -> node_id
        bool stop_election;
        bool force_stop;
        map<int,int>tns_to_client;
        set<int>tns_log;
        set<int> commit_tns_unique;
        set<int> exec_tns_unique;
        map<int,int>exec_status; // unique -> status
        set<int>tns;
        set<int>ignore_vc_ballot;
        std::unordered_map<std::string, Transaction> digest_to_transaction;
        mutable std::mutex digest_map_mutex;
        std::chrono::milliseconds timing_attack_delay;
        enum class TxStatus : int {
            None = 0,
            PrePrepared,
            Prepared,
            Committed,
            Executed
        };

        // void PrintStatus(int seq);
    map<int, std::set<int>> pbft_prepare_votes;
    map<int, std::map<std::string, std::vector<Prepare>>> pbft_prepare_messages;
    map<int, std::map<std::string, std::set<int>>> pbft_prepare_senders;
    std::set<std::pair<int, std::string>> pbft_prepare_certificates_sent;
    mutex pbft_prepare_mutex;
    // Commit aggregation structures
    map<int, std::map<std::string, std::vector<CommitLeader>>> pbft_commit_messages;
    map<int, std::map<std::string, std::set<int>>> pbft_commit_senders;
    std::set<std::pair<int, std::string>> pbft_commit_certificates_sent;
    mutex pbft_commit_mutex;
    // View change tracking
    std::map<int, ViewChange> view_change_by_sender;
    bool view_change_local_timer_triggered;
    bool view_change_broadcasted;
    int active_view_change_ballot;
    std::set<int> new_view_broadcasted_ballots;
    mutex view_change_mutex;
        CryptoPP::RSA::PrivateKey private_key;
        CryptoPP::RSA::PublicKey public_key;
        map<int, CryptoPP::RSA::PublicKey> peer_public_keys;
        mutex key_mutex;
    static Node* instance;
private:
    class AsyncCallBase;
    class AsyncSendClientTransactionCall;
    class AsyncClientReadCall;
public:
        // mutex accepted_seq_count_mutex;
        // int accepted_seq_val_count;
        void* tag; bool ok;


        struct AsyncClientCall {
            TransCompleted req;
            Ack reply;
            ClientContext ctx;
            Status status;
            unique_ptr<ClientAsyncResponseReader<Ack>> rpc;
            int unique_id;
        };

        struct AsyncPrepareLeaderCall {
            std::shared_ptr<ClientContext> context;
            std::shared_ptr<CompletionQueue> completion_queue;
            std::shared_ptr<Ack> reply;
            std::shared_ptr<Status> status;
            std::shared_ptr<Prepare> request;
            std::unique_ptr<ClientAsyncResponseReader<Ack>> responder;
            int receiver_id;
        };

        struct AsyncPrepareFollowerCall {
            std::shared_ptr<ClientContext> context;
            std::shared_ptr<CompletionQueue> completion_queue;
            std::shared_ptr<Ack> reply;
            std::shared_ptr<Status> status;
            std::shared_ptr<PrepareCertificate> request;
            std::unique_ptr<ClientAsyncResponseReader<Ack>> responder;
            int receiver_id;
        };

        struct AsyncCommitLeaderCall {
            std::shared_ptr<ClientContext> context;
            std::shared_ptr<CompletionQueue> completion_queue;
            std::shared_ptr<Ack> reply;
            std::shared_ptr<Status> status;
            std::shared_ptr<CommitLeader> request;
            std::unique_ptr<ClientAsyncResponseReader<Ack>> responder;
            int receiver_id;
        };

        struct AsyncCommitFollowerCall {
            std::shared_ptr<ClientContext> context;
            std::shared_ptr<CompletionQueue> completion_queue;
            std::shared_ptr<Ack> reply;
            std::shared_ptr<Status> status;
            std::shared_ptr<CommitCertificate> request;
            std::unique_ptr<ClientAsyncResponseReader<Ack>> responder;
            int receiver_id;
        };

        struct AsyncViewChangeCall {
            std::shared_ptr<ClientContext> context;
            std::shared_ptr<CompletionQueue> completion_queue;
            std::shared_ptr<Ack> reply;
            std::shared_ptr<Status> status;
            std::shared_ptr<ViewChange> request;
            std::unique_ptr<ClientAsyncResponseReader<Ack>> responder;
            int receiver_id;
        };

        struct AsyncNewViewCall {
            std::shared_ptr<ClientContext> context;
            std::shared_ptr<CompletionQueue> completion_queue;
            std::shared_ptr<Ack> reply;
            std::shared_ptr<Status> status;
            std::shared_ptr<NewView> request;
            std::unique_ptr<ClientAsyncResponseReader<Ack>> responder;
            int receiver_id;
        };

        // Node (map<int,shared_ptr<msg::NodeService::Stub>> stubs,int node_id,int nodes_num){
    Node (int node_id,int nodes_num,map<string,int>db, int client_num,
        const CryptoPP::RSA::PrivateKey& priv_key,
        const map<int, CryptoPP::RSA::PublicKey>& peer_keys){

        instance = this;
        signal(SIGINT, Node::signalHandler);
        // signal(SIGTERM, Node::signalHandler);

        // signal(SIGSEGV, Node::signalHandler);
        // signal(SIGABRT, Node::signalHandler);

        std::atexit([](){
            if (Node::instance) {
                pid_t pid = getpid();
                if (Node::instance->is_leader)
                    std::cout << "[Leader Node " << Node::instance->node_id<< " | pid " << pid << "] exiting" << std::endl;
                else
                    std::cout << "[Node " << Node::instance->node_id<< " | pid " << pid << "] exiting" << std::endl;
            }
        });


        string temp="db/node_"+to_string(node_id)+"_db.txt";
        std::ofstream outfile(temp, std::ios::out | std::ios::trunc);
        outfile<<"";
        outfile.close();


        this->service_ = new msg::NodeService::AsyncService();
        cout<<"Node : "<<node_id<<" created\n"<<endl;
        // stubs.erase(this->node_id);
        // sleep(2);
        this->force_stop=false;
        this->db=move(db);
        this->node_id=node_id;
        this->nodes_num=nodes_num;

        this->crash=false;
        this->dark=false;
        this->sign=false;
        this->equivocation=false;
        this->time_delay=false;
        this->view_change_broadcasted=false;
        this->view_change_local_timer_triggered=false;

        this->private_key = priv_key;
        this->public_key.Initialize(this->private_key.GetModulus(), this->private_key.GetPublicExponent());
        {
            std::lock_guard<std::mutex> lock(this->key_mutex);
            this->peer_public_keys = peer_keys;
            this->peer_public_keys.erase(this->node_id);
        }
        this->ballot_num=1;
        this->cur_leader=1;
        this->execution_index=0;
        this->seq_num=1;
        this->is_leader=false;
        this->node_server=thread(&Node::run_server,this,this->node_id);
        this->node_server.detach();
        this->node_async_server=thread(&Node::run_async_server,this,this->node_id);
        this->node_async_server.detach();
        this->timer_stopped=false;
        this->reset_flag=false;
        this->running_election=false;
        this->trigger_tns=false;
        this->stop_process=false;
        this->stop_vc_timer=true;
        this->block_msg_vc=false;
        this->active_view_change_ballot=-1;
        this->reset_vc_timer=false;

    const int min_timeout_ms = 15 * 1000;
    const int max_timeout_ms = 30 * 1000;
    // Space timeouts so higher node_ids wait longer, producing ascending timers.
        int step_ms = 0;
        if (nodes_num > 1) {
            step_ms = (max_timeout_ms - min_timeout_ms) / (nodes_num - 1);
        }
        this->random_timeout = min_timeout_ms + step_ms * (std::max(0, this->node_id - 1));
        if (this->random_timeout > max_timeout_ms) {
            this->random_timeout = max_timeout_ms;
        }
        this->timeout = std::chrono::milliseconds(random_timeout);
        this->heartbeat_timer=std::chrono::milliseconds(2000);
        this->timing_attack_delay = std::chrono::seconds(5);
        if (this->timing_attack_delay.count() == 0) {
            this->timing_attack_delay = std::chrono::milliseconds(1);
        }
        // this->heartbeat_timer = std::chrono::milliseconds(500);

        for (int k = 1; k <= nodes_num; ++k) {
            if(k==this->node_id) continue;
            string address = "localhost:" + to_string(5000 + k);
            auto channel = CreateChannel(address, InsecureChannelCredentials());
            auto stub = msg::NodeService::NewStub(channel);
            this->stubs.emplace(k, move(stub));
        }

        for (int k = 1; k <= client_num; ++k) {
            string address = "localhost:" + to_string(6100 + k);
            auto channel = CreateChannel(address, InsecureChannelCredentials());
            auto stub = msg::NodeService::NewStub(channel);
            this->client_stubs.emplace(k, move(stub));
        }

        // sleep(2);
        if (node_id==1){
            sleep(2);
            // this->client_reqs.push(Client_Req({{"A","B"},10},0,1));
            // logs_mutex.lock();
            // seq_num_mutex.lock();
            // this->logs.push_back(Log({0,0},this->seq_num,this->client_reqs.front().tns));
            // this->seq_num++;
            // seq_num_mutex.unlock();
            // logs_mutex.unlock();
            this->timer_stopped=true;
            this->is_leader=true;
            thread(&Node::start_election, this).detach(); //Testing
        }
        // else{
        this->timer_stopped=true;
        this->node_timer=thread(&Node::start_timer,this);
        this->node_timer.detach();



        // }

        // this->cur_leader=0;
        // cout<<"Node"<<endl;
    }

    ~Node() {
        // cout<<"Shutting Down Async"<<endl;
        cout<<this->node_id<<"Destroyed"<<endl;
        if (is_leader)
            std::cout << "[Leader Node " << node_id << "] failed (destroyed or exiting)" << std::endl;
        else
            std::cout << "[Node " << node_id << "] shutting down" << std::endl;

        if (server_) server_->Shutdown();
        if (cq_) cq_->Shutdown();
        if (async_server_thread_.joinable()) async_server_thread_.join();
        delete service_;
    }

    static void signalHandler(int sig) {
        if (instance != nullptr) {
            if (instance->is_leader)
                std::cout << "[Leader Node " << instance->node_id<< "] failed (signal " << sig << ")" << std::endl;
            else
                std::cout << "[Node " << instance->node_id<< "] shutting down (signal " << sig << ")" << std::endl;
        }
        exit(0);
    }

    
    void keep_main_process_alive(){
        unique_lock<mutex> lock(main_process);
        while(cv_main_process.wait_for(lock,chrono::seconds(1),[this](){
            return !this->force_stop;
        }));
    }

    void run_server(int node_id) {
        ServerBuilder builder;
        string address = "localhost:" + to_string(5000 + node_id);
        builder.AddListeningPort(address, InsecureServerCredentials());
        builder.RegisterService(this);
        unique_ptr<Server> server(builder.BuildAndStart());
        server->Wait();
    }

    void run_async_server(int node_id) {
        if (!service_) service_ = new msg::NodeService::AsyncService();

        ServerBuilder builder;
        string address = "localhost:" + to_string(5100 + node_id);
        builder.AddListeningPort(address, InsecureServerCredentials());

        builder.RegisterService(service_);

        cq_ = builder.AddCompletionQueue();

        server_ = builder.BuildAndStart();

        async_server_thread_ = std::thread(&Node::HandleRpcs, this);
        server_->Wait(); // if you want this thread to block here
    }

    void HandleRpcs() {
        new AsyncSendClientTransactionCall(service_, cq_.get(), this);
        new AsyncClientReadCall(service_, cq_.get(), this);
        void* tag = nullptr;
        bool ok = false;
        while (cq_->Next(&tag, &ok)) {
            if (!tag) {
                continue;
            }
            static_cast<AsyncCallBase*>(tag)->Proceed(ok);
        }
    }


    void start_vc_timer() {
        unique_lock<mutex> lock(time_vc_mutex);

        cv_vc_timer.wait_for(lock, timeout);
        
        if(this->reset_vc_timer){
            this->stop_vc_timer=true;
            return;
        }
        
        if(this->stop_vc_timer)return;
        
        std::cout << "Node " << this->node_id << " VC timer expired after " << timeout.count() << "ms, starting view change\n";
        this->stop_vc_timer=true;
        // this->view_change_by_sender.clear();
        this->new_view_broadcasted_ballots.clear();
        this->trigger_view_change_due_to_timeout();
    }

    void start_timer() {
        unique_lock<mutex> lock(time_mutex);

        // if(this->node_id!=1)this->timer_stopped=false;
        // else this->timer_stopped=true;

        while (true) {
            bool timed_out = !cv.wait_for(lock, timeout, [this]() { return this->reset_flag; });

            // unique_lock<mutex> lock_vote(vote_mutex);
            // cv_vote.wait_for(lock_vote, std::chrono::milliseconds(50), [this]() { return this->reset_flag; });

            if(this->timer_stopped) timed_out=false;
            while (this->timer_stopped) {
                cv.wait_for(lock, timeout);
            }

            reset_flag_mutex.lock();
            if (this->reset_flag) {
                this->reset_flag = false;
                reset_flag_mutex.unlock();
                continue; // restart timer with new randomized duration
            }
            reset_flag_mutex.unlock();

            if (timed_out) {
                std::cout << "Node " << this->node_id << " timer expired after " << timeout.count() << "ms, starting view change\n";
                this->trigger_view_change_due_to_timeout();
                // stopped_mutex.lock();
                this->timer_stopped = true;
                // stopped_mutex.unlock();
            }
        }
    }

    void reset_timer() {
        unique_lock<mutex> lock(time_mutex);
        if(this->running_election)return;
        // cout<<"Resetted Timer"<<endl;
        this->reset_flag = true;
        stopped_mutex.lock();
        this->timer_stopped = false;
        stopped_mutex.unlock();
        cv.notify_one();
    }
    
    // Stop election timer
    void stop_timer() {
        unique_lock<mutex> lock(time_mutex);       
        stopped_mutex.lock();
        this->timer_stopped = true;
        stopped_mutex.unlock();
        lock.unlock();
        cv.notify_one();
    }

    void start_election() { 
        cout<<"Leader started at Node :"<<node_id<<"\n";
        this->is_leader=true;
        // this->running_election=true;
        this->stop_election=false;
        // this->election_completed=false;
        this->trigger_tns=false;
        
        if(!this->is_leader||this->stop_election){
            this->election_completed=false;
            this->running_election=false;
            this->stop_election=false;
            this->votes=0;
            this->cur_leader=-1;
            this->is_leader=false;
            queue<Client_Req>().swap(this->client_reqs);
            // election_threads.clear();
        }
        else{
            while(!this->stop_process&&this->is_leader){
                if(this->trigger_tns&&!this->client_reqs.empty()){

                    int current_seq;
                    if(this->equivocation){
                        current_seq = this->seq_num + 1;
                    }
                    else current_seq = this->seq_num;

                    Client_Req pending_req = this->client_reqs.front();
                    this->client_reqs.pop();

                    // if(this->tns_log.find(pending_req.unique)!=this->tns_log.end()){
                    //     // cout<<"Duplicate Client Request with unique id :"<<pending_req.unique<<" from Client :"<<pending_req.client_id<<endl;
                    //     continue;
                    // }

                    // cout<<"Trying to Inserted in Log"<<endl;
                    this->logs.resize(current_seq); // Ensure logs vector is large enough
                    this->logs[current_seq-1]=Log({this->ballot_num,this->node_id}, current_seq, pending_req.tns, pending_req.unique);
                    // cout<<"Inserted in Log"<<endl;
                    this->update_sequence_status(current_seq, TxStatus::PrePrepared);

                    Transaction txn_msg;
                    Clients* txn_clients = txn_msg.mutable_c();
                    txn_clients->set_c1(pending_req.tns.first.first);
                    txn_clients->set_c2(pending_req.tns.first.second);
                    txn_msg.set_amt(pending_req.tns.second);
                    txn_msg.set_unique(pending_req.unique);

                    std::string digest = this->compute_transaction_digest(txn_msg);
                    this->remember_transaction_digest(digest, txn_msg);
                    std::string signature = this->sign_payload(digest);

                    auto preprepare_req = std::make_shared<PrePrepare>();
                    preprepare_req->set_type("PRE-PREPARE");
                    Ballot* pp_ballot = preprepare_req->mutable_b();
                    pp_ballot->set_ballot_num(this->ballot_num);
                    pp_ballot->set_sender_id(this->node_id);
                    preprepare_req->set_seq_num(current_seq);
                    preprepare_req->set_digest(digest);
                    preprepare_req->set_signature(signature);
                    preprepare_req->mutable_tns()->CopyFrom(txn_msg);

                    if(pending_req.unique != -1){
                        this->tns_log.insert(pending_req.unique);
                        if(!pending_req.tns.first.first.empty()){
                            this->tns_to_client[pending_req.unique] = pending_req.tns.first.first[0]-'A'+1;
                        }
                    }

                    // cout<<"Node :"<<node_id<<" Sending PrePrepare for Log Entry:"<<current_seq<<" digest "<<this->digest_to_hex(digest)<<endl;
                    for (auto& [pid, stub] : this->stubs) {
                        if(pid==this->node_id||(this->dark&&this->dark_set.find(pid)!=this->dark_set.end())) continue;
                        thread(&Node::send_preprepare, this, pid, preprepare_req).detach();
                    }
                    // cout<<"Node :"<<node_id<<" Sent PrePrepare"<<endl;

                    this->seq_num = current_seq + 1;
                    if(this->client_reqs.empty())this->trigger_tns=false;
                }
                else{
                    this->trigger_tns=false;
                    unique_lock<mutex> logs_lock(temp_mutex);
                    cv_log_entry_or_heatbeat.wait_for(logs_lock,chrono::milliseconds(this->heartbeat_timer));
                    // cout<<"Hi"<<endl;
                    if(this->stop_process||this->stop_election||!this->is_leader)break;
                    // if((!this->trigger_tns)){
                        // cout<<"Triggering Heartbeat"<<endl;
                        // for (auto& [pid, stub] : this->stubs) {
                        //     if(pid==this->node_id) continue;
                        //     thread(&Node::send_heartbeat, this, pid).detach();
                        // }
                        // cout<<"Ending Heartbeat"<<endl;
                    // }
                }
            }
        }
        if(this->cur_leader==this->node_id)this->cur_leader=-1;
        this->is_leader=false;
        this->running_election=false;
        this->stop_election=false;
        this->election_completed=false;
        this->trigger_tns=false;
        this->votes=0;
        queue<Client_Req>().swap(this->client_reqs);
        cout<<"Leader "<<this->node_id<<" Stepping down"<<endl;
        this->reset_view_change_state();
        // if(!this->stop_process){
        //     // thread(&Node::reset_timer,this).detach();
        //     this->reset_timer();
        // }
    }

    void send_prepare_leader(int receiver_id, const Prepare& req){
        if (this->stop_process) return;
        if (receiver_id == this->node_id) return;

        auto it = this->stubs.find(receiver_id);
        if (it == this->stubs.end()) {
            std::cerr << "Node " << this->node_id << " has no stub for leader " << receiver_id << std::endl;
            return;
        }

        // this->delay_timing_attack();

        auto call = std::make_shared<AsyncPrepareLeaderCall>();
        call->context = std::make_shared<ClientContext>();
        call->completion_queue = std::make_shared<CompletionQueue>();
        call->reply = std::make_shared<Ack>();
        call->status = std::make_shared<Status>();
        call->request = std::make_shared<Prepare>(req);
        call->receiver_id = receiver_id;
    call->responder = it->second->AsyncSendPrepareLeader(call->context.get(), *call->request, call->completion_queue.get());
        cout<<"Node :"<<this->node_id<<" Sending Prepare to Leader "<<receiver_id<<" for Log Entry:"<<req.seq_num()<<endl;
        call->responder->Finish(call->reply.get(), call->status.get(), (void*)nullptr);

    }

    void send_prepare_follower(int receiver_id, const PrepareCertificate& req) {
        if (this->stop_process) return;
        if (receiver_id == this->node_id) return;

        auto it = this->stubs.find(receiver_id);
        if (it == this->stubs.end()) {
            std::cerr << "Node " << this->node_id << " has no stub for peer " << receiver_id << std::endl;
            return;
        }

        if(this->time_delay)this->delay_timing_attack();

        auto call = std::make_shared<AsyncPrepareFollowerCall>();
        call->context = std::make_shared<ClientContext>();
        call->completion_queue = std::make_shared<CompletionQueue>();
        call->reply = std::make_shared<Ack>();
        call->status = std::make_shared<Status>();
        call->request = std::make_shared<PrepareCertificate>(req);
        call->receiver_id = receiver_id;
    call->responder = it->second->AsyncSendPrepareFollower(call->context.get(), *call->request, call->completion_queue.get());
        call->responder->Finish(call->reply.get(), call->status.get(), (void*)nullptr);
    }

    void send_commit_leader(int receiver_id, const CommitLeader& req) {
        if (this->stop_process) return;
        if (receiver_id == this->node_id) return;

        auto it = this->stubs.find(receiver_id);
        if (it == this->stubs.end()) {
            std::cerr << "Node " << this->node_id << " has no stub for leader " << receiver_id << std::endl;
            return;
        }

        auto call = std::make_shared<AsyncCommitLeaderCall>();
        call->context = std::make_shared<ClientContext>();
        call->completion_queue = std::make_shared<CompletionQueue>();
        call->reply = std::make_shared<Ack>();
        call->status = std::make_shared<Status>();
        call->request = std::make_shared<CommitLeader>(req);
        call->receiver_id = receiver_id;
    call->responder = it->second->AsyncSendCommitLeader(call->context.get(), *call->request, call->completion_queue.get());
        call->responder->Finish(call->reply.get(), call->status.get(), (void*)nullptr);

    }

    void send_commit_follower(int receiver_id, const CommitCertificate& req) {
        if (this->stop_process) return;
        if (receiver_id == this->node_id) return;

        auto it = this->stubs.find(receiver_id);
        if (it == this->stubs.end()) {
            std::cerr << "Node " << this->node_id << " has no stub for peer " << receiver_id << std::endl;
            return;
        }

        if(this->time_delay)this->delay_timing_attack();

        auto call = std::make_shared<AsyncCommitFollowerCall>();
        call->context = std::make_shared<ClientContext>();
        call->completion_queue = std::make_shared<CompletionQueue>();
        call->reply = std::make_shared<Ack>();
        call->status = std::make_shared<Status>();
        call->request = std::make_shared<CommitCertificate>(req);
        call->receiver_id = receiver_id;
    call->responder = it->second->AsyncSendCommitFollower(call->context.get(), *call->request, call->completion_queue.get());
        call->responder->Finish(call->reply.get(), call->status.get(), (void*)nullptr);
    }

    void send_preprepare(int peer_id, std::shared_ptr<PrePrepare> req) {
        if (this->stop_process) return;

        Acknowledge reply;
        ClientContext context;
        Status status;

        if(this->time_delay)this->delay_timing_attack();

        if (this->equivocation && this->equi_set.find(peer_id) != this->equi_set.end()) {
            PrePrepare message = *req; // copy so equivocation tweaks stay peer-local
            message.set_seq_num(message.seq_num()-1);
            status = stubs[peer_id]->SendPrePrepare(&context, message, &reply);
        }
        else status = stubs[peer_id]->SendPrePrepare(&context, *req, &reply);

        if (this->stop_process) return;
        if (!status.ok()) return;
        if (this->ballot_num > req->b().ballot_num()) return;

        // if (reply.vote()) {
        //     this->merge_logs_from_ack(reply);
        // }
    }


    void send_commit(int peer_id, int seq_num){

        Commit req;
        Ack reply;
        ClientContext context;
        
        req.set_seq_num(seq_num);
        req.mutable_b()->set_ballot_num(this->ballot_num);
        req.mutable_b()->set_sender_id(this->node_id);
        req.mutable_tns()->mutable_c()->set_c1(this->logs[seq_num-1].tns.first.first);
        req.mutable_tns()->mutable_c()->set_c2(this->logs[seq_num-1].tns.first.second);
        req.mutable_tns()->set_amt(this->logs[seq_num-1].tns.second);
        req.mutable_tns()->set_unique(this->logs[seq_num-1].unique);

        if(this->time_delay)this->delay_timing_attack();

        Status status = stubs[peer_id]->SendCommit(&context, req, &reply);
        if(this->stop_process)return;
    }

    void dump_logs(std::ostream& os = std::cout) {
        // Create folder if it doesn’t exist
        std::string folder = "logs";
        mkdir(folder.c_str(), 0777);  // ignored if already exists

        // Build file name for this node
        std::string filename = folder + "/node_" + std::to_string(this->node_id) + "_logs.txt";

        // Clear file at start (truncate mode)
        std::ofstream outfile(filename, std::ios::out | std::ios::trunc);
        if (!outfile.is_open()) {
            os << "Error: Could not open " << filename << "\n";
            return;
        }

        // Write logs
        outfile << "\n================= LOG DUMP (Node " << this->node_id << ") =================\n";
        if (logs.empty()) {
            outfile << "(No logs available)\n";
        } else {
            for (size_t i = 0; i < this->logs.size(); ++i) {
                const auto& log = this->logs[i];
                outfile << "Log[" << std::setw(2) << i << "]\n";
                outfile << "  Ballot     : (" << log.b.first << ", " << log.b.second << ")\n";
                outfile << "  SeqNum     : " << log.seq_num << "\n";
                outfile << "  Transaction: "
                        << log.tns.first.first << " -> " << log.tns.first.second
                        << " | Amt=" << log.tns.second << "\n";
                outfile << "  Unique     : " << log.unique << "\n";
                outfile << "  Committed     : " << (this->commit_set.find(log.seq_num)!=this->commit_set.end()) << "\n";
                outfile << "--------------------------------------------\n";
            }
        }
        outfile << "============================================\n\n";
        outfile << "Execution Index : " << std::setw(2) << this->execution_index << "\n";

        outfile.close();
        os << "Logs written to " << filename << "\n";
    }


    void dump_new_view(NewView message, std::string context, std::vector<ViewChange> received_view_changes) {
        mkdir("view", 0777);
        std::lock_guard<std::mutex> file_lock(this->view_log_file_mutex);
        const std::string filename = "view/logs_new_view.txt";

        std::ofstream outfile(filename, std::ios::out | std::ios::app);
        if (!outfile.is_open()) {
            std::cerr << "Node " << this->node_id
                      << " unable to open " << filename << " for new-view logging" << std::endl;
            return;
        }

    const auto now = std::chrono::system_clock::now();
        const std::time_t now_time = std::chrono::system_clock::to_time_t(now);
        std::tm tm_snapshot{};
#if defined(_WIN32)
        localtime_s(&tm_snapshot, &now_time);
#else
        localtime_r(&now_time, &tm_snapshot);
#endif

    auto format_transaction = [this](const std::string& digest_bytes) {
        std::ostringstream oss;
        const std::string hex_digest = this->digest_to_hex(digest_bytes);
        auto maybe_txn = this->lookup_transaction_by_digest(digest_bytes);
        if (!maybe_txn.has_value()) {
        oss << "txn=(unknown) digest=" << hex_digest;
        return oss.str();
        }
        const Transaction& txn = *maybe_txn;
        oss << "txn=" << txn.c().c1() << "->" << txn.c().c2()
        << " amt=" << txn.amt() << " unique=" << txn.unique()
        << " digest=" << hex_digest;
        return oss.str();
    };

        outfile << "\n===== NewView Dump (node n" << this->node_id << ") @ "
                << std::put_time(&tm_snapshot, "%Y-%m-%d %H:%M:%S") << " =====\n";
        outfile << "Context : " << context << "\n";
        outfile << "Ballot  : " << message.b().ballot_num()
                << "  Leader: n" << message.b().sender_id() << "\n";
        outfile << "ViewChanges: " << message.view_changes_size()
                << "  PreparedCerts: " << message.prepared_size()
                << "  CommitCerts: " << message.committed_size() << "\n";

        if (message.view_changes_size() > 0) {
            outfile << "-- View Change Entries --\n";
            for (int idx = 0; idx < message.view_changes_size(); ++idx) {
                const auto& vc = message.view_changes(idx);
                outfile << "  [" << std::setw(2) << idx << "] sender=n" << vc.b().sender_id()
                        << " ballot=" << vc.b().ballot_num()
                        << " prepared=" << vc.prepared_size()
                        << " committed=" << vc.committed_size() << "\n";
            }
        }

        if (message.prepared_size() > 0) {
            outfile << "-- Prepared Certificates --\n";
            for (int idx = 0; idx < message.prepared_size(); ++idx) {
                const auto& cert = message.prepared(idx);
                outfile << "  [" << std::setw(2) << idx << "] seq=" << cert.seq_num()
                        << " sender=n" << cert.sender_id()
                        << " prepares=" << cert.prepares_size() << "\n";
                for (int p = 0; p < cert.prepares_size(); ++p) {
                    const auto& prep = cert.prepares(p);
                    outfile << "       prepare from n" << prep.b().sender_id()
                            << " seq=" << prep.seq_num()
                            << " ballot=" << prep.b().ballot_num()
                            << " " << format_transaction(prep.digest()) << "\n";
                }
            }
        }

        if (message.committed_size() > 0) {
            outfile << "-- Commit Certificates --\n";
            for (int idx = 0; idx < message.committed_size(); ++idx) {
                const auto& cert = message.committed(idx);
                outfile << "  [" << std::setw(2) << idx << "] seq=" << cert.seq_num()
                        << " sender=n" << cert.sender_id()
                        << " commits=" << cert.commits_size() << "\n";
                for (int c = 0; c < cert.commits_size(); ++c) {
                    const auto& commit = cert.commits(c);
                    outfile << "       commit from n" << commit.sender_id()
                            << " seq=" << commit.seq_num()
                            << " ballot=" << commit.b().ballot_num()
                            << " " << format_transaction(commit.digest()) << "\n";
                }
            }
        }

        outfile << "-- View Change Messages Observed (local store) --\n";
        if (received_view_changes.empty()) {
            outfile << "  (none)\n";
        } else {
            for (std::size_t idx = 0; idx < received_view_changes.size(); ++idx) {
                const auto& vc = received_view_changes[idx];
                outfile << "  [" << std::setw(2) << idx << "] sender=n" << vc.b().sender_id()
                        << " ballot=" << vc.b().ballot_num()
                        << " prepared=" << vc.prepared_size()
                        << " committed=" << vc.committed_size() << "\n";

                if (vc.prepared_size() > 0) {
                    outfile << "     Prepared certificates:\n";
                    for (int p = 0; p < vc.prepared_size(); ++p) {
                        const auto& cert = vc.prepared(p);
                        outfile << "       seq=" << cert.seq_num()
                                << " sender=n" << cert.sender_id()
                                << " prepares=" << cert.prepares_size() << "\n";
                        for (int m = 0; m < cert.prepares_size(); ++m) {
                            const auto& prep = cert.prepares(m);
                            outfile << "         · from n" << prep.b().sender_id()
                                    << " seq=" << prep.seq_num()
                                    << " ballot=" << prep.b().ballot_num()
                                    << " " << format_transaction(prep.digest()) << "\n";
                        }
                    }
                }

                if (vc.committed_size() > 0) {
                    outfile << "     Commit certificates:\n";
                    for (int c = 0; c < vc.committed_size(); ++c) {
                        const auto& cert = vc.committed(c);
                        outfile << "       seq=" << cert.seq_num()
                                << " sender=n" << cert.sender_id()
                                << " commits=" << cert.commits_size() << "\n";
                        for (int m = 0; m < cert.commits_size(); ++m) {
                            const auto& commit = cert.commits(m);
                            outfile << "         · from n" << commit.sender_id()
                                    << " seq=" << commit.seq_num()
                                    << " ballot=" << commit.b().ballot_num()
                                    << " " << format_transaction(commit.digest()) << "\n";
                        }
                    }
                }
            }
        }

        outfile << "===== End NewView Dump =====\n";
        outfile.close();
    }

    void dump_db(ostream& os = cout) {

        string filename = "db/node_" + to_string(this->node_id) + "_db.txt";

        ofstream ofs(filename, ios::app);  // append mode
        if (!ofs.is_open()) {
            os << "Error: Unable to open " << filename << " for appending.\n";
            return;
        }

        ofs << "\nKey-Value pairs in DB:\n";
        if (this->db.empty()) {
            ofs << "(DB is empty)\n";
        } else {
            for (const auto& [key, value] : this->db) {
                ofs << "  " << setw(5) << key << " : " << value << "\n";
            }
        }
        ofs << "=================================================\n";
        ofs.close();

        os << "[INFO] Dumped DB for node " << node_id << " → " << filename << "\n";
    }

    void forward_to_leader(ClientTransaction *req) {
        if(this->cur_leader==this->node_id||(this->dark&&this->dark_set.find(this->cur_leader)!=this->dark_set.end())) return;

        ClientTransaction new_req;
        ClientContext context;
        CompletionQueue cq;
        TransAck reply;
        Status status;
        new_req.mutable_tns()->CopyFrom(req->tns());
        new_req.set_c1(req->c1());
        new_req.set_unique(req->unique());

        std::unique_ptr<ClientAsyncResponseReader<TransAck> > rpc(this->client_stubs[this->cur_leader]->AsyncSendClientTransaction(&context,new_req,&cq));
        rpc->Finish(&reply, &status, (void*)nullptr);
    }

    Status SendPrePrepare(ServerContext*c, const PrePrepare*req, Acknowledge*reply) override {
        if (this->stop_process||this->block_msg_vc) return Status::CANCELLED;
        unique_lock<mutex> lock_vote(vote_mutex);
        unique_lock<mutex> lock(processing_msg);
        if (this->stop_process||this->block_msg_vc) return Status::CANCELLED;

        // reply->set_vote(false);

        try {
            const std::string computed_digest = this->compute_transaction_digest(req->tns());
            if (computed_digest != req->digest()) {
                std::cerr << "Node " << this->node_id << " rejected PrePrepare from "
                          << req->b().sender_id() << " due to digest mismatch (expected "
                          << this->digest_to_hex(computed_digest) << " got "
                          << this->digest_to_hex(req->digest()) << ")" << std::endl;
                return Status::OK;
            }

            if (!this->verify_peer_signature(req->b().sender_id(), req->digest(), req->signature())) {
                std::cerr << "Node " << this->node_id << " rejected PrePrepare from "
                          << req->b().sender_id() << " due to invalid signature" << std::endl;
                return Status::OK;
            }

            if (req->b().ballot_num() < this->ballot_num) {
                return Status::OK;
            }

            Transaction txn_copy;
            txn_copy.CopyFrom(req->tns());
            this->remember_transaction_digest(req->digest(), txn_copy);

            const int seq = req->seq_num();
            if (seq <= 0) {
                std::cerr << "Node " << this->node_id << " received invalid sequence number "
                          << seq << " in PrePrepare" << std::endl;
                return Status::OK;
            }

            Prepare prepare_msg;
            prepare_msg.set_type("PBFT_PREPARE");
            Ballot* prepare_ballot = prepare_msg.mutable_b();
            prepare_ballot->set_ballot_num(req->b().ballot_num());
            prepare_ballot->set_sender_id(this->node_id);
            prepare_msg.set_seq_num(seq);
            prepare_msg.set_digest(req->digest());
            const std::string prepare_payload = this->build_prepare_payload(prepare_ballot->ballot_num(), seq, prepare_msg.digest());
            prepare_msg.set_signature(this->sign_payload(prepare_payload));

            {
                std::lock_guard<std::mutex> log_guard(logs_mutex);
                if (static_cast<int>(this->logs.size()) < seq) {
                    this->logs.resize(seq, Log());
                }

                if (this->logs[seq - 1].unique != -1) {
                    this->tns_log.erase(this->logs[seq - 1].unique);
                    this->tns_to_client.erase(this->logs[seq - 1].unique);
                }
                this->logs[seq - 1] = Log({req->b().ballot_num(), req->b().sender_id()}, seq,
                                        {{req->tns().c().c1(), req->tns().c().c2()}, req->tns().amt()},
                                        req->tns().unique());
                this->tns_log.insert(req->tns().unique());
                if (!req->tns().c().c1().empty()) {
                    this->tns_to_client[req->tns().unique()] = req->tns().c().c1()[0] - 'A' + 1;
                }
            }

            this->update_sequence_status(seq, TxStatus::PrePrepared);

            if(!this->crash){
                if (this->node_id != req->b().sender_id() && !(this->dark && this->dark_set.find(req->b().sender_id()) != this->dark_set.end())) {
                    std::thread(&Node::send_prepare_leader, this, req->b().sender_id(), prepare_msg).detach();
                }
            }

            {
                std::lock_guard<std::mutex> ack_lock(this->pbft_prepare_mutex);
                this->pbft_prepare_votes[seq].insert(this->node_id);
            }

            {
                std::lock_guard<std::mutex> seq_lock(seq_num_mutex);
                this->seq_num = std::max(this->seq_num, seq + 1);
            }

            // std::cout << "Node " << this->node_id << " accepted PrePrepare from "
            //         << req->b().sender_id() << " for seq " << seq << " digest "
            //         << this->digest_to_hex(req->digest()) << std::endl;

            if(this->timer_stopped)this->reset_timer();
        } catch (const std::exception& ex) {
            std::cerr << "Node " << this->node_id << " encountered error handling PrePrepare: "
                    << ex.what() << std::endl;
        }

        if(this->timer_stopped==true)this->reset_timer();

        return Status::OK;
    }

    Status SendPrepareLeader(ServerContext* c, const Prepare* req, Ack* reply) override {
        if (this->stop_process||this->block_msg_vc) return Status::CANCELLED;
        unique_lock<mutex> processing_lock(processing_msg);
        if (this->stop_process||this->block_msg_vc) return Status::CANCELLED;

        const int follower_id = req->b().sender_id();
        const int ballot = req->b().ballot_num();
        const int seq = req->seq_num();
        const std::string& digest = req->digest();

        const std::string payload = this->build_prepare_payload(ballot, seq, digest);
        if (!this->verify_peer_signature(follower_id, payload, req->signature())) {
            std::cerr << "Leader " << this->node_id << " rejected prepare from node "
                      << follower_id << " due to invalid signature" << std::endl;
            return Status::OK;
        }

        if (ballot < this->ballot_num) {
            return Status::OK;
        }

        if (this->ballot_num < ballot) {
            this->ballot_num = ballot;
        }

        this->record_prepare_response(*req);

        if (reply) {
            reply->set_type("PBFT_PREPARE_LEADER_ACK");
            reply->set_seq_num(seq);
            reply->set_sender_id(this->node_id);
        }

        // std::cout << "Leader " << this->node_id << " recorded prepare from node "
                //   << follower_id << " for seq " << seq << std::endl;
        return Status::OK;
    }

    Status SendPrepareFollower(ServerContext* c, const PrepareCertificate* req, Ack* reply) override {
        if (this->stop_process||this->block_msg_vc) return Status::CANCELLED;
        unique_lock<mutex> processing_lock(processing_msg);
        if (this->stop_process||this->block_msg_vc) return Status::CANCELLED;
        if(this->crash)return Status::OK;

        const int seq = req->seq_num();
        const int ballot = req->b().ballot_num();
    std::set<int> certificate_senders;
    std::string certificate_digest;

        if (req->prepares_size() == 0) {
            std::cerr << "Node " << this->node_id
                      << " rejected prepare certificate for seq " << seq
                      << " from leader " << req->sender_id()
                      << " because it contained no prepare messages" << std::endl;
            return Status::OK;
        }
        Log local_log;
        bool log_found = false;
        {
            std::lock_guard<std::mutex> log_guard(this->logs_mutex);
            if (seq > 0 && seq <= static_cast<int>(this->logs.size())) {
                const Log& candidate = this->logs[seq - 1];
                if (candidate.seq_num == seq) {
                    local_log = candidate;
                    log_found = true;
                }
            }
            else {
                return Status::OK;
            }
        }

        if (!log_found) {
            std::cerr << "Node " << this->node_id
                      << " missing log entry for seq " << seq
                      << " when processing prepare certificate from leader "
                      << req->sender_id() << "; deferring validation" << std::endl;
            return Status::OK;
        }

        Transaction txn_msg;
        txn_msg.mutable_c()->set_c1(local_log.tns.first.first);
        txn_msg.mutable_c()->set_c2(local_log.tns.first.second);
        txn_msg.set_amt(local_log.tns.second);
        txn_msg.set_unique(local_log.unique);

        std::string local_digest;
        local_digest = this->compute_transaction_digest(txn_msg);
    this->remember_transaction_digest(local_digest, txn_msg);

        for (const auto& prepare_msg : req->prepares()) {
            const int sender_id = prepare_msg.b().sender_id();
            const std::string& prepare_digest = prepare_msg.digest();
            const std::string payload = this->build_prepare_payload(
                prepare_msg.b().ballot_num(), prepare_msg.seq_num(), prepare_digest);
            if (!this->verify_peer_signature(sender_id, payload, prepare_msg.signature())) {
                std::cerr << "Node " << this->node_id
                          << " rejected prepare certificate from leader " << req->sender_id()
                          << " due to invalid prepare from node " << sender_id << std::endl;
                return Status::OK;
            }

            if (prepare_msg.seq_num() != seq) {
                std::cerr << "Node " << this->node_id
                          << " rejected prepare certificate for seq " << seq
                          << " because it contained mismatched sequence "
                          << prepare_msg.seq_num() << std::endl;
                return Status::OK;
            }
            certificate_digest = prepare_msg.digest();
            if (certificate_digest != local_digest) {
            std::cerr << "Node " << this->node_id
                      << " rejected prepare certificate for seq " << seq
                      << " due to digest mismatch (local "
                      << this->digest_to_hex(local_digest) << ", certificate "
                      << this->digest_to_hex(certificate_digest) << ")" << std::endl;
            return Status::OK;
            }
            else{
                certificate_senders.insert(sender_id);
            }
        }

        const int f = std::max(1, (this->nodes_num - 1) / 3);
        const int required = std::max(1, 2 * f);
        if (static_cast<int>(certificate_senders.size()) < required) {
            std::cerr << "Node " << this->node_id
                      << " rejected prepare certificate for seq " << seq
                      << " because it only had " << certificate_senders.size()
                      << " distinct prepares (need at least " << required << ")" << std::endl;
            return Status::OK;
        }

        {
            std::lock_guard<std::mutex> lock(this->pbft_prepare_mutex);
            auto& vote_set = this->pbft_prepare_votes[seq];
            vote_set.insert(this->node_id);
            vote_set.insert(certificate_senders.begin(), certificate_senders.end());
        }

        this->update_sequence_status(seq, TxStatus::Prepared);

        CommitLeader commit_msg;
        Ballot* commit_ballot = commit_msg.mutable_b();
        commit_ballot->set_ballot_num(ballot);
        commit_ballot->set_sender_id(this->node_id);
        commit_msg.set_seq_num(seq);
        commit_msg.set_sender_id(this->node_id);
        commit_msg.set_digest(local_digest);
        const std::string commit_payload = this->build_prepare_payload(ballot, seq, local_digest);
        commit_msg.set_signature(this->sign_payload(commit_payload));

        bool already_sent = false;
        {
            std::lock_guard<std::mutex> lock(this->pbft_commit_mutex);
            auto& sender_set = this->pbft_commit_senders[seq][local_digest];
            if (!sender_set.insert(this->node_id).second) {
                already_sent = true;
            } else {
                this->pbft_commit_messages[seq][local_digest].push_back(commit_msg);
            }
        }

        if(!(this->dark&&this->dark_set.find(req->sender_id())==this->dark_set.end())) this->send_commit_leader(req->sender_id(), commit_msg);

        if (reply) {
            reply->set_type("PBFT_PREPARE_FOLLOWER_ACK");
            reply->set_seq_num(seq);
            reply->set_sender_id(this->node_id);
        }

        return Status::OK;
    }

    Status SendCommitLeader(ServerContext* c, const CommitLeader* req, Ack* reply) override {
        if (this->stop_process||this->block_msg_vc) return Status::CANCELLED;
        unique_lock<mutex> processing_lock(processing_msg);
        if (this->stop_process||this->block_msg_vc) return Status::CANCELLED;

        const int follower_id = req->sender_id();
        const int ballot = req->b().ballot_num();
        const int seq = req->seq_num();
        const std::string digest((const char*)req->digest().data(), req->digest().size());

        const std::string payload = this->build_prepare_payload(ballot, seq, digest);
        if (!this->verify_peer_signature(follower_id, payload, req->signature())) {
            std::cerr << "Leader " << this->node_id << " rejected commit from node "
                      << follower_id << " due to invalid signature" << std::endl;
            return Status::OK;
        }

        if (ballot < this->ballot_num) return Status::OK;
        // if (this->ballot_num < ballot) this->ballot_num = ballot;

        this->record_commit_response(*req);

        if (reply) {
            reply->set_type("PBFT_COMMIT_LEADER_ACK");
            reply->set_seq_num(seq);
            reply->set_sender_id(this->node_id);
        }

        // std::cout << "Leader " << this->node_id << " recorded commit from node "
                //   << follower_id << " for seq " << seq << std::endl;
        return Status::OK;
    }

    Status SendCommitFollower(ServerContext* c, const CommitCertificate* req, Ack* reply) override {
        if (this->stop_process||this->block_msg_vc) return Status::CANCELLED;
        unique_lock<mutex> processing_lock(processing_msg);
        if (this->stop_process||this->block_msg_vc) return Status::CANCELLED;
        if(this->crash)return Status::OK;

        const int seq = req->seq_num();
        const int ballot = req->b().ballot_num();

        std::set<int> certificate_senders;

        for (const auto& commit_msg : req->commits()) {
            const int sender_id = commit_msg.sender_id();
            const std::string digest((const char*)commit_msg.digest().data(), commit_msg.digest().size());
            const std::string payload = this->build_prepare_payload(commit_msg.b().ballot_num(), commit_msg.seq_num(), digest);
            if (!this->verify_peer_signature(sender_id, payload, commit_msg.signature())) {
                std::cerr << "Node " << this->node_id
                          << " rejected commit certificate from leader " << req->sender_id()
                          << " due to invalid commit from node " << sender_id << std::endl;
                return Status::OK;
            }
            certificate_senders.insert(sender_id);
            // this->pbft_prepare_votes[seq].insert(sender_id);
        }

        bool reached_quorum = false;
        {
            std::lock_guard<std::mutex> lock(this->pbft_commit_mutex);
            auto& vote_set = this->pbft_prepare_votes[seq];
            vote_set.insert(this->node_id);
            vote_set.insert(certificate_senders.begin(), certificate_senders.end());

            const int f = std::max(1, (this->nodes_num - 1) / 3);
            const int required = std::max(1, 2 * f);
            if (static_cast<int>(vote_set.size()) >= required) {
                reached_quorum = true;
            }
        }

        if (reached_quorum) {
            this->apply_commit_and_execute(seq,true);
        }

        if (reply) {
            reply->set_type("PBFT_COMMIT_FOLLOWER_ACK");
            reply->set_seq_num(seq);
            reply->set_sender_id(this->node_id);
        }

        // std::cout << "Node " << this->node_id << " accepted commit certificate for seq " << seq << std::endl;
        return Status::OK;
    }

    Status SendViewChange(ServerContext* c, const ViewChange* req, Ack* reply) override {
        if (this->stop_process) return Status::CANCELLED;
        const int sender_id = req->b().sender_id();
        const int ballot = req->b().ballot_num();

        std::lock_guard<std::mutex> lock(this->view_change_mutex);
        if(this->ignore_vc_ballot.find(ballot)!=this->ignore_vc_ballot.end()){
            return Status::OK;
        }
        
        bool should_broadcast_view_change = false;
        bool should_broadcast_new_view = false;
        int count_for_ballot = 0;

        {
            if (this->active_view_change_ballot != -1) {
                
                if (ballot < this->active_view_change_ballot) {
                    return Status::OK;
                }
                
                if (ballot > this->active_view_change_ballot) {
                    // return Status::OK;
                    // this->view_change_by_sender.clear();
                    // this->view_change_broadcasted = false;
                    // this->view_change_local_timer_triggered = false;
                    // this->new_view_broadcasted_ballots.erase(ballot);
                    // this->active_view_change_ballot = ballot;
                }
            }
            else{
                this->view_change_local_timer_triggered=false;
                this->view_change_broadcasted=false;
            }

            auto it = this->view_change_by_sender.find(sender_id);
            if (it == this->view_change_by_sender.end()) {
                this->view_change_by_sender[sender_id] = *req;
            }

            for (auto vc_it = this->view_change_by_sender.begin(); vc_it != this->view_change_by_sender.end(); vc_it++) {
                if (vc_it->second.b().ballot_num() == ballot) count_for_ballot++;
            }

            const int f = std::max(1, (this->nodes_num - 1) / 3);
            const int threshold_broadcast = std::max(1, f + 1);
            const int threshold_new_view = std::max(1, 2 * f + 1);

            // if(this->node_id==2)cout<<"Count for ballot "<<ballot<<" :"<<count_for_ballot<<endl;

            if (static_cast<int>(this->view_change_by_sender.size()) == threshold_broadcast) {
                
                if(!this->view_change_local_timer_triggered &&!this->view_change_broadcasted){
                    this->view_change_broadcasted = true;
                    should_broadcast_view_change = true;
                    if(this->is_leader){
                        this->is_leader=false;
                        cv_log_entry_or_heatbeat.notify_one();
                    }
                }
            }

            if (count_for_ballot == threshold_new_view) {
                // cout<<"Node "<<this->node_id<<" starting VC timer "<<ballot<<endl;
                this->reset_vc_timer=true;
                if(!this->stop_vc_timer){
                    cv_vc_timer.notify_one();
                }
                while(!this->stop_vc_timer);
                this->reset_vc_timer=false;

                this->ignore_vc_ballot.insert(ballot);
                // cout<<"Node "<<this->node_id<<" started VC timer "<<ballot<<endl;
                if(this->leader_for_ballot(ballot) == this->node_id){

                    should_broadcast_new_view = true;
                    // snapshot.reserve(this->view_change_by_sender.size());
                    // for (const auto& [sid, vc] : this->view_change_by_sender) {
                        //     snapshot.push_back(vc);
                        // }
                    this->new_view_broadcasted_ballots.insert(ballot);
                }
                else{
                    this->view_change_by_sender.clear();
                    // cout<<"Node "<<this->node_id<<" stopped prior VC timer "<<ballot<<endl;
                    this->stop_vc_timer=false;
                    std::thread(&Node::start_vc_timer, this).detach();
                }
            }
        }
        // if(this->node_id==2)cout<<"Hi Node "<<this->node_id<<" from " <<sender_id<<endl;

        if (!this->crash&&should_broadcast_view_change) {
            cout<<"Node :"<<this->node_id<<" Triggered view-change broadcast for ballot "<<ballot<<endl;
            if(this->timer_stopped==false){
                // lock_guard<mutex> lock(time_mutex);
                this->stop_timer();
            }

            // cout<<"Node "<<this->node_id<<" Building view-change bcz of f+1"<<endl;
            int min_ballot=-1;
            for(auto x: this->view_change_by_sender){
                // cout<<x.second.b().ballot_num()<<endl;
                if(min_ballot<0) min_ballot=x.second.b().ballot_num();
                else min_ballot=min(min_ballot,x.second.b().ballot_num());
            }

            // cout<<"Node "<<this->node_id<<" Building view-change for ballot "<<min_ballot<<" bcz of f+1"<<endl;
            
            this->active_view_change_ballot=min_ballot;
            ViewChange local_message = this->build_view_change_message(min_ballot);
            // cout<<"Node "<<this->node_id<<" Built view-change for ballot "<<min_ballot<<" bcz of f+1"<<endl;
            {
                // std::lock_guard<std::mutex> lock(this->view_change_mutex);
                this->view_change_by_sender[this->node_id] = local_message;
                this->view_change_local_timer_triggered = true;
            }
            std::cout << "Node " << this->node_id
                      << " sending view change for ballot " << ballot << std::endl;
            this->block_msg_vc=true;
            // if(this->is_leader){
            //     this->is_leader=false;
            //     cv_log_entry_or_heatbeat.notify_one();
            // }
            this->broadcast_view_change(local_message);
            // this->maybe_start_election_from_view_change(ballot);
        }

        if (should_broadcast_new_view) {
            // cout<<"Leader "<<this->node_id<<" Preparing to build new-view for ballot "<<ballot<<endl;
            {
                lock_guard<mutex> lock(time_vc_mutex);
                this->stop_vc_timer=true;
                cv_vc_timer.notify_one();
            }
            this->block_msg_vc=false;
            std::vector<ViewChange> local_snapshot;
            {
                // std::lock_guard<std::mutex> lock(this->view_change_mutex);
                auto it_self = this->view_change_by_sender.find(this->node_id);
                if (it_self == this->view_change_by_sender.end()) {
                    ViewChange local_message = this->build_view_change_message(ballot);
                    this->view_change_by_sender[this->node_id] = local_message;
                }
                local_snapshot.reserve(this->view_change_by_sender.size());
                for (const auto& [sid, vc] : this->view_change_by_sender) {
                    local_snapshot.push_back(vc);
                }
            }
            // cout<<"Leader "<<this->node_id<<" Building log for new-view for ballot "<<ballot<<endl;
            this->rebuild_log_from_view_changes(local_snapshot);
            // cout<<"Leader "<<this->node_id<<" Built log for new-view for ballot "<<ballot<<endl;

            if(!this->crash){
                // cout<<"Leader "<<this->node_id<<" Building new-view for ballot "<<ballot<<endl;
                NewView new_view_message = this->build_new_view_message(ballot, local_snapshot);
                // cout<<"Leader "<<this->node_id<<" Built new-view for ballot "<<ballot<<endl;
                // if (!this->apply_new_view(new_view_message)) {
                //     std::cerr << "Leader " << this->node_id
                //               << " failed to apply locally generated NewView for ballot "
                //               << ballot << std::endl;
                // } else {
                    std::string context = std::string("Broadcast new-view for ballot ") + std::to_string(ballot);
                    std::thread(&Node::dump_new_view, this, new_view_message, context, local_snapshot).detach();
                    std::cout << "Leader " << this->node_id << " broadcasting NewView for ballot " << ballot << std::endl;
                    this->broadcast_new_view(new_view_message);
                // }
            }

            this->view_change_by_sender.clear();

            {
                std::lock_guard<std::mutex> election_lock(this->vote_mutex);
                if (!this->is_leader) {
                    this->is_leader=true;
                    this->cur_leader=this->node_id;
                    std::thread(&Node::start_election, this).detach();
                }
            }
        }

        reply->set_type("VIEW-CHANGE-ACK");
        reply->set_sender_id(this->node_id);
        reply->set_seq_num(0);
        return Status::OK;
    }

    void send_new_view_after_timeout(int ballot){

        this->ignore_vc_ballot.insert(ballot);

        // cout<<"Leader "<<this->node_id<<" Preparing to build new-view for ballot "<<ballot<<endl;
            {
                lock_guard<mutex> lock(time_vc_mutex);
                this->stop_vc_timer=true;
                cv_vc_timer.notify_one();
            }
            this->block_msg_vc=false;
            std::vector<ViewChange> local_snapshot;
            {
                // std::lock_guard<std::mutex> lock(this->view_change_mutex);
                auto it_self = this->view_change_by_sender.find(this->node_id);
                if (it_self == this->view_change_by_sender.end()) {
                    ViewChange local_message = this->build_view_change_message(ballot);
                    this->view_change_by_sender[this->node_id] = local_message;
                }
                local_snapshot.reserve(this->view_change_by_sender.size());
                for (const auto& [sid, vc] : this->view_change_by_sender) {
                    local_snapshot.push_back(vc);
                }
            }
            // cout<<"Leader "<<this->node_id<<" Building log for new-view for ballot "<<ballot<<endl;
            this->rebuild_log_from_view_changes(local_snapshot);
            // cout<<"Leader "<<this->node_id<<" Built log for new-view for ballot "<<ballot<<endl;

            if(!this->crash){
                // cout<<"Leader "<<this->node_id<<" Building new-view for ballot "<<ballot<<endl;
                NewView new_view_message = this->build_new_view_message(ballot, local_snapshot);
                    std::string context = std::string("Broadcast new-view for ballot ") + std::to_string(ballot);
                    std::thread(&Node::dump_new_view, this, new_view_message, context, local_snapshot).detach();
                    std::cout << "Leader " << this->node_id << " broadcasting NewView for ballot " << ballot << std::endl;
                    this->broadcast_new_view(new_view_message);
                // }
            }

            this->view_change_by_sender.clear();

            {
                std::lock_guard<std::mutex> election_lock(this->vote_mutex);
                if (!this->is_leader) {
                    this->is_leader=true;
                    this->cur_leader=this->node_id;
                    std::thread(&Node::start_election, this).detach();
                }
            }
    }

    Status SendNewView(ServerContext* c, const NewView* req, Ack* reply) override {
        if (this->stop_process) return Status::CANCELLED;
        if(stop_vc_timer==false){
            lock_guard<mutex> lock(time_vc_mutex);
            this->stop_vc_timer=true;
            cv_vc_timer.notify_one();
        }
        if(this->node_id==6)cout<<"Node "<<this->node_id<<" Received New-View for ballot "<<req->b().ballot_num()<<endl;
        this->view_change_broadcasted=false;
        this->view_change_local_timer_triggered=false;
        this->apply_new_view(*req);

        if (reply) {
            reply->set_type("NEW-VIEW-ACK");
            reply->set_sender_id(this->node_id);
            reply->set_seq_num(0);
        }

        return Status::OK;
    }

    Status SendNewLogView(ServerContext*c, const NewLogView*req, ServerWriter<Accepted>* writer) override {
        if(this->stop_process)return Status::CANCELLED;
        unique_lock<mutex>lock(processing_msg);
        if(this->stop_process)return Status::CANCELLED;
        if(this->ballot_num<=req->b().ballot_num()){

            if(this->ballot_num<req->b().ballot_num()&&this->is_leader){
                this->is_leader=false;

                this->stop_election=true;
                this->cur_leader=req->b().sender_id();
                cout<<"Node "<<this->node_id<<" Stepping Down"<<endl;
            }
            
            this->logs.resize(req->log().size(), Log());
            seq_num_mutex.lock();
            int ctr=0;
            // this->seq_num=-1;
            for (const auto& t : req->log()){
                // if (context->IsCancelled()) break; // handle client cancellation
                Accepted reply;
                reply.set_sender_id(this->node_id);
                reply.set_seq_num(t.seq_num());
                Ballot* b = reply.mutable_b();
                b->set_ballot_num(t.b().ballot_num());
                b->set_sender_id(t.b().sender_id());
                Transaction* temp_tns = reply.mutable_tns();
                Clients* temp_c = temp_tns->mutable_c();
                temp_c->set_c1(t.tns().c().c1());
                temp_c->set_c2(t.tns().c().c2());
                temp_tns->set_amt(t.tns().amt());
                temp_tns->set_unique(t.tns().unique());

                if(t.seq_num()>this->execution_index||this->commit_set.find(t.seq_num())==this->commit_set.end()){

                    logs_mutex.lock();

                    if(this->logs[ctr].seq_num!=-1){
                        this->tns_log.erase(this->logs[ctr].unique);
                        this->tns_to_client.erase(this->logs[ctr].unique);
                    }
                    this->tns_log.insert(t.tns().unique());
                    this->tns_to_client[t.tns().unique()]=t.tns().c().c1()[0]-'A'+1;

                    logs[ctr].b={t.b().ballot_num(),t.b().sender_id()};
                    logs[ctr].seq_num=t.seq_num();
                    logs[ctr].tns={{t.tns().c().c1(),t.tns().c().c2()},t.tns().amt()};
                    logs[ctr].unique=t.tns().unique();
                    this->seq_num=max(this->seq_num,logs[ctr].seq_num+1);

                    logs_mutex.unlock();
                }
                writer->Write(reply); // send one update
                ctr++;
            }
            seq_num_mutex.unlock();
        }
        return Status::OK;
    }

    Status ActionCommand(ServerContext*c, const Command*req, Ack*reply) override{
        // lock_guard<mutex> lock(stop_process_mutex);
        if(req->type()=="StopProcess"){
            stop_process_mutex.lock();
            this->stop_process=true;
            this->stop_timer();
            this->cur_leader=-1;
            if(this->is_leader){
                this->stop_election=true;
                cv_log_entry_or_heatbeat.notify_one();
            }
            reply->set_type("Process Stopped");  //Add some logic here to resume after stopping
            cout<<"Node :"<<this->node_id<<" Process Stopped\n"<<endl;
            stop_process_mutex.unlock();
        }
        else if(req->type()=="TriggerTns"){
            if(this->is_leader){
                this->trigger_tns=true;
                cv_log_entry_or_heatbeat.notify_one();
            }
        }
        else if(req->type()=="LF"){
            if(this->is_leader){
                processing_msg.lock();
                stop_process_mutex.lock();
                this->stop_process=true;
                this->stop_election=true;
                this->stop_timer();
                this->cur_leader=-1;
                cv_log_entry_or_heatbeat.notify_one();
                stop_process_mutex.unlock();
                processing_msg.unlock();
                reply->set_type("Leader");
            }
            else reply->set_type("Node");
        }
        else if(req->type()=="ResetNode"){
            {
                std::lock_guard<std::mutex> log_guard(this->logs_mutex);
                this->logs.clear();
            }
            {
                std::lock_guard<std::mutex> seq_guard(this->seq_num_mutex);
                this->seq_num = 1;
            }
            {
                std::lock_guard<std::mutex> commit_guard(this->commit_set_mutex);
                this->commit_set.clear();
                this->commit_tns_unique.clear();
            }
            this->tns_log.clear();
            this->tns_to_client.clear();
            this->tns.clear();
            this->exec_tns_unique.clear();
            this->execution_index = 0;
            this->clear_consensus_tracking();
            {
                std::lock_guard<std::mutex> digest_guard(this->digest_map_mutex);
                this->digest_to_transaction.clear();
            }
            {
                std::lock_guard<std::mutex> view_guard(this->view_change_mutex);
                this->view_change_by_sender.clear();
                this->view_change_local_timer_triggered = false;
                this->view_change_broadcasted = false;
                this->active_view_change_ballot = -1;
                this->new_view_broadcasted_ballots.clear();
                this->ignore_vc_ballot.clear();
            }
            this->ballot_num = 1;
            this->cur_leader = 1;
            this->running_election = false;
            this->stop_election = false;
            {
                std::lock_guard<std::mutex> stop_guard(this->stop_process_mutex);
                this->stop_process = false;
            }
            this->block_msg_vc=false;
            this->trigger_tns = false;
            this->force_stop = false;
            this->crash = false;
            this->dark = false;
            this->sign = false;
            this->time_delay = false;
            this->equivocation = false;
            this->dark_set.clear();
            this->equi_set.clear();
            this->election_map.clear();
            while (!this->client_reqs.empty()) {
                this->client_reqs.pop();
            }
            this->exec_status.clear();
            this->clear_sequence_status();
            this->db.clear();
            if(this->node_id==1) {
                if(!this->is_leader){
                    this->is_leader=true;
                    thread( &Node::start_election, this).detach();
                }
            }
            else if(this->is_leader){
                this->is_leader=false;
                cv_log_entry_or_heatbeat.notify_one();
            }
            for(char c='A';c<='J';c++){
                string s(1,c);
                this->db[s]=10;
            }
            this->stop_timer();
            this->reset_flag=true;
            // this->reset_timer();

            reply->set_type("Node Reset Done");
            cout<<"Node :"<<this->node_id<<" Reset Done\n"<<endl;
        }
        else if(req->type()=="crash"){
            this->crash=true;
            reply->set_type("Node Crashed");
            cout<<"Node :"<<this->node_id<<" Crashed\n"<<endl;
        }
        else if(req->type()=="sign"){
            this->sign=true;
            reply->set_type("Node Invalid Signature");
            cout<<"Node :"<<this->node_id<<" Invalid Signature\n"<<endl;
        }
        else if(req->type()=="time"){
            this->time_delay=true;
            reply->set_type("Leader Delay");
            cout<<"Node :"<<this->node_id<<" Timing Delay\n"<<endl;
        }
        else if(req->type().rfind("dark", 0) == 0){
            this->dark=true;
            this->dark_set.clear();

            auto trim_token = [](std::string& s){
                auto not_space = [](unsigned char ch){ return !std::isspace(ch); };
                s.erase(s.begin(), std::find_if(s.begin(), s.end(), not_space));
                s.erase(std::find_if(s.rbegin(), s.rend(), not_space).base(), s.end());
            };

            const std::string& cmd = req->type();
            auto open_pos = cmd.find('(');
            auto close_pos = cmd.find(')', open_pos == std::string::npos ? 0 : open_pos + 1);
            if (open_pos != std::string::npos && close_pos != std::string::npos && close_pos > open_pos + 1) {
                std::string inside = cmd.substr(open_pos + 1, close_pos - open_pos - 1);
                std::stringstream ss(inside);
                std::string token;
                while (std::getline(ss, token, ',')) {
                    trim_token(token);
                    token.erase(std::remove_if(token.begin(), token.end(), [](unsigned char ch){
                        return ch == 'n' || ch == 'N';
                    }), token.end());
                    trim_token(token);
                    if (token.empty()) {
                        continue;
                    }
                    try {
                        int node_idx = std::stoi(token);
                        if (node_idx > 0) {
                            cout<<"Node "<<this->node_id<<" Dark Target Node :"<<node_idx<<endl;
                            this->dark_set.insert(node_idx);
                        }
                    } catch (const std::exception& ex) {
                        std::cerr << "Node " << this->node_id
                                  << " unable to parse dark node token '" << token
                                  << "': " << ex.what() << std::endl;
                    }
                }
            }

            if (this->dark_set.empty()) {
                std::cout << "Node " << this->node_id
                          << " activated dark mode with no targets (command: "
                          << cmd << ")" << std::endl;
            } else {
                std::cout << "Node " << this->node_id
                          << " activated dark mode for nodes";
                for (int node_idx : this->dark_set) {
                    std::cout << " n" << node_idx;
                }
                std::cout << std::endl;
            }
        }

        else if(req->type().rfind("equivocation", 0) == 0){
            this->equivocation=true;
            this->equi_set.clear();

            auto trim_token = [](std::string& s){
                auto not_space = [](unsigned char ch){ return !std::isspace(ch); };
                s.erase(s.begin(), std::find_if(s.begin(), s.end(), not_space));
                s.erase(std::find_if(s.rbegin(), s.rend(), not_space).base(), s.end());
            };

            const std::string& cmd = req->type();
            auto open_pos = cmd.find('(');
            auto close_pos = cmd.find(')', open_pos == std::string::npos ? 0 : open_pos + 1);
            if (open_pos != std::string::npos && close_pos != std::string::npos && close_pos > open_pos + 1) {
                std::string inside = cmd.substr(open_pos + 1, close_pos - open_pos - 1);
                std::stringstream ss(inside);
                std::string token;
                while (std::getline(ss, token, ',')) {
                    trim_token(token);
                    token.erase(std::remove_if(token.begin(), token.end(), [](unsigned char ch){
                        return ch == 'n' || ch == 'N';
                    }), token.end());
                    trim_token(token);
                    if (token.empty()) {
                        continue;
                    }
                    try {
                        int node_idx = std::stoi(token);
                        if (node_idx > 0) {
                            this->equi_set.insert(node_idx);
                        }
                    } catch (const std::exception& ex) {
                        std::cerr << "Node " << this->node_id
                                  << " unable to parse equivocation node token '" << token
                                  << "': " << ex.what() << std::endl;
                    }
                }
            }

            if (this->equi_set.empty()) {
                std::cout << "Node " << this->node_id
                          << " activated equivocation with no specific targets (command: "
                          << cmd << ")" << std::endl;
            } else {
                std::cout << "Node " << this->node_id
                          << " will equivocate for nodes";
                for (int node_idx : this->equi_set) {
                    std::cout << " n" << node_idx;
                }
                std::cout << std::endl;
            }
        }
        else if(req->type()=="PrintLog"){
            this->dump_logs();
        }
        else if(req->type()=="PrintDB"){
            this->dump_db();
        }
        else if(req->type()=="PrintStatus"){
            this->PrintStatus(req->seq_num());
        }
        else if(req->type()=="ResumeProcess"){
            this->stop_process=false;
            this->stop_election=false;
            // this->reset_timer();
            reply->set_type("Process Resumed");
            cout<<"Node :"<<this->node_id<<" Process Resumed\n"<<endl;
        }
        return Status::OK;
    }

    void PrintStatus(int seq) {
        if (seq <= 0) {
            std::cout << "Node " << this->node_id << " sequence " << seq << " status: X" << std::endl;
            return;
        }

        TxStatus status = this->current_sequence_status(seq);
        if (status == TxStatus::None) {
            {
                std::lock_guard<std::mutex> lock(this->commit_set_mutex);
                if (seq <= this->execution_index) {
                    status = TxStatus::Executed;
                } else if (this->commit_set.find(seq) != this->commit_set.end()) {
                    status = TxStatus::Committed;
                }
            }

            if (status == TxStatus::None) {
                std::lock_guard<std::mutex> log_lock(this->logs_mutex);
                if (seq > 0 && seq <= static_cast<int>(this->logs.size()) &&
                    this->logs[seq - 1].seq_num == seq) {
                    status = TxStatus::PrePrepared;
                }
            }
        }

        std::cout << "Node " << this->node_id << " sequence " << seq << " status: "
                  << this->status_label(status) << std::endl;
    }

    private:
        std::map<int, TxStatus> sequence_status;
        mutable std::mutex sequence_status_mutex;

        void update_sequence_status(int seq, TxStatus status) {
            if (seq <= 0) return;
            std::lock_guard<std::mutex> lock(this->sequence_status_mutex);
            auto it = this->sequence_status.find(seq);
            if (it == this->sequence_status.end() ||
                static_cast<int>(status) > static_cast<int>(it->second)) {
                this->sequence_status[seq] = status;
            }
        }

        void clear_sequence_status() {
            std::lock_guard<std::mutex> lock(this->sequence_status_mutex);
            this->sequence_status.clear();
        }

        TxStatus current_sequence_status(int seq) const {
            if (seq <= 0) return TxStatus::None;
            std::lock_guard<std::mutex> lock(this->sequence_status_mutex);
            auto it = this->sequence_status.find(seq);
            if (it == this->sequence_status.end()) return TxStatus::None;
            return it->second;
        }

        std::string status_label(TxStatus status) const {
            switch (status) {
                case TxStatus::PrePrepared: return "PP";
                case TxStatus::Prepared: return "P";
                case TxStatus::Committed: return "C";
                case TxStatus::Executed: return "E";
                default: return "X";
            }
        }

        void delay_timing_attack() const {
            if (!this->time_delay || !this->is_leader) {
                return;
            }
            if (this->timing_attack_delay.count() > 0) {
                std::this_thread::sleep_for(this->timing_attack_delay);
            }
        }

        struct DigestAggregateInfo {
            std::unordered_set<int> unique_senders;
            int highest_ballot = -1;
            int highest_sender = -1;
            bool has_prepare = false;
            bool has_commit = false;
        };

        struct SeqAggregateInfo {
            std::unordered_map<std::string, DigestAggregateInfo> digest_map;
        };

        struct ReconstructedEntry {
            Log log_entry;
            bool marked_committed = false;
            bool marked_prepared = false;
        };


    std::optional<CryptoPP::RSA::PublicKey> get_peer_public_key(int peer_id) {
        std::lock_guard<std::mutex> lock(this->key_mutex);
        auto it = this->peer_public_keys.find(peer_id);
        if (it != this->peer_public_keys.end()) {
            return it->second;
        }
        return std::nullopt;
    }

    void record_prepare_response(const Prepare& prepare_msg) {
        const int sender_id = prepare_msg.b().sender_id();
        const int seq = prepare_msg.seq_num();
        const std::string digest_key = prepare_msg.digest();

        std::vector<Prepare> certificate_payload;
        std::set<int> sender_snapshot;
        int certificate_ballot = prepare_msg.b().ballot_num();
        bool should_broadcast = false;

        {
            std::lock_guard<std::mutex> lock(this->pbft_prepare_mutex);
            auto& sender_set = this->pbft_prepare_senders[seq][digest_key];
            if (!sender_set.insert(sender_id).second) {
                return; // duplicate prepare from same replica
            }

            auto& messages = this->pbft_prepare_messages[seq][digest_key];
            messages.push_back(prepare_msg);

            const int f = std::max(1, (this->nodes_num - 1) / 3);
            const int required = std::max(1, 2 * f);
            const std::pair<int, std::string> cert_key{seq, digest_key};

            if(static_cast<int>(sender_set.size()) > required) return;
            else if (static_cast<int>(sender_set.size()) == required &&
                this->pbft_prepare_certificates_sent.insert(cert_key).second) {
                certificate_payload = messages;
                sender_snapshot = sender_set;
                should_broadcast = true;
            }
        }

        if (!should_broadcast) {
            return;
        }

        if(this->crash)return;
        this->update_sequence_status(seq, TxStatus::Prepared);

        PrepareCertificate certificate;
        Ballot* cert_ballot = certificate.mutable_b();
        cert_ballot->set_ballot_num(certificate_ballot);
        cert_ballot->set_sender_id(this->node_id);
        certificate.set_seq_num(seq);
        certificate.set_sender_id(this->node_id);

        for (const auto& msg : certificate_payload) {
            Prepare* added = certificate.add_prepares();
            added->CopyFrom(msg);
            added->set_digest(msg.digest());
            added->set_signature(msg.signature());
        }

        {
            std::lock_guard<std::mutex> lock(this->pbft_prepare_mutex);
            auto& vote_set = this->pbft_prepare_votes[seq];
            vote_set.insert(this->node_id);
            vote_set.insert(sender_snapshot.begin(), sender_snapshot.end());
        }

        std::cout << "Leader " << this->node_id
                  << " obtained 2f prepare responses for seq " << seq
                  << ", broadcasting prepare certificate" << std::endl;
        this->broadcast_prepare_certificate(certificate);
    }

    void broadcast_prepare_certificate(const PrepareCertificate& certificate) {
        for (auto& [peer_id, stub] : this->stubs) {
            if(peer_id==this->node_id||(this->dark&&this->dark_set.find(peer_id)!=this->dark_set.end())) continue;
            std::thread(&Node::send_prepare_follower, this, peer_id, certificate).detach();
        }
    }

    void record_commit_response(const CommitLeader& commit_msg) {
        const int sender_id = commit_msg.sender_id();
        const int seq = commit_msg.seq_num();
        const std::string digest_key = commit_msg.digest();

        std::vector<CommitLeader> certificate_payload;
        std::set<int> sender_snapshot;
        int certificate_ballot = commit_msg.b().ballot_num();
        bool should_broadcast = false;

        {
            std::lock_guard<std::mutex> lock(this->pbft_commit_mutex);
            auto& sender_set = this->pbft_commit_senders[seq][digest_key];
            if (!sender_set.insert(sender_id).second) {
                return; // duplicate commit from same replica
            }

            auto& messages = this->pbft_commit_messages[seq][digest_key];
            messages.push_back(commit_msg);

            const int f = std::max(1, (this->nodes_num - 1) / 3);
            const int required = std::max(1, 2 * f);
            const std::pair<int, std::string> cert_key{seq, digest_key};

            if(static_cast<int>(sender_set.size()) > required) return;
            else if (static_cast<int>(sender_set.size()) == required &&
                this->pbft_commit_certificates_sent.insert(cert_key).second) {
                certificate_payload = messages;
                sender_snapshot = sender_set;
                should_broadcast = true;
            }
        }

        if (!should_broadcast) {
            return;
        }
        if(this->crash)return;

        CommitCertificate certificate;
        Ballot* cert_ballot = certificate.mutable_b();
        cert_ballot->set_ballot_num(certificate_ballot);
        cert_ballot->set_sender_id(this->node_id);
        certificate.set_seq_num(seq);
        certificate.set_sender_id(this->node_id);

        for (const auto& msg : certificate_payload) {
            CommitLeader* added = certificate.add_commits();
            added->CopyFrom(msg);
            added->set_digest(msg.digest());
            added->set_signature(msg.signature());
        }

        {
            std::lock_guard<std::mutex> lock(this->pbft_commit_mutex);
            auto& vote_set = this->pbft_prepare_votes[seq];
            vote_set.insert(this->node_id);
            vote_set.insert(sender_snapshot.begin(), sender_snapshot.end());
        }

        this->apply_commit_and_execute(seq,false);

        std::cout << "Leader " << this->node_id
                  << " obtained 2f commit responses for seq " << seq
                  << ", broadcasting commit certificate" << std::endl;

        this->broadcast_commit_certificate(certificate);
    }

    void broadcast_commit_certificate(const CommitCertificate& certificate) {
        for (auto& [peer_id, stub] : this->stubs) {
            if(peer_id==this->node_id||(this->dark&&this->dark_set.find(peer_id)!=this->dark_set.end())) continue;
            std::thread(&Node::send_commit_follower, this, peer_id, certificate).detach();
        }
    }

    void apply_commit_and_execute(int seq,bool follower=false) {
        std::lock_guard<std::mutex> guard(this->commit_set_mutex);
        if (seq <= 0 || seq > static_cast<int>(this->logs.size())) {
            return;
        }

        Log& entry = this->logs[seq - 1];
        if (entry.seq_num != seq) {
            return;
        }

        this->commit_set.insert(seq);
        this->commit_tns_unique.insert(entry.unique);
        this->update_sequence_status(seq, TxStatus::Committed);

        while (this->commit_set.find(this->execution_index + 1) != this->commit_set.end()) {
            Log& ready = this->logs[this->execution_index];
            bool success = true;
            if (ready.tns.first.first != "noop") {
                const std::string& src = ready.tns.first.first;
                const std::string& dst = ready.tns.first.second;
                int amt = ready.tns.second;
                if (this->db[src] > amt) {
                    this->db[src] -= amt;
                    this->db[dst] += amt;
                    success = true;
                } else {
                    success = false;
                }
            }

            if (ready.unique != -1) {
                this->exec_status[ready.unique] = success ? 1 : 0;
            }
            cout<<"Node :"<<this->node_id<<" Executed Seq Num :"<<this->execution_index+1<<" with status :"<<(success?"Success":"Failure")<<endl;

            this->update_sequence_status(ready.seq_num, TxStatus::Executed);
            this->notify_client_of_execution(ready, success);
            this->execution_index++;
            if(follower){
                if (this->seq_num-1 == this->execution_index) {
                    this->stop_timer();
                    cout<<"Node :"<<this->node_id<<" Timer Stopped"<<endl;
                } else {
                    this->reset_timer();
                }
            }
        }
    }

    int leader_for_ballot(int ballot) const {
        return ((ballot-1) % this->nodes_num) + 1;
    }

    std::vector<PrepareCertificate> collect_prepared_certificates_for_view_change() {
        std::vector<PrepareCertificate> certificates;
        const int executed_upto = this->execution_index;
        const int f = std::max(1, (this->nodes_num - 1) / 3);
        const int required = std::max(1, 2 * f);

        std::lock_guard<std::mutex> lock(this->pbft_prepare_mutex);
        for (const auto& [seq, digest_map] : this->pbft_prepare_messages) {
            if (seq <= executed_upto) {
                continue;
            }
            for (const auto& [digest, messages] : digest_map) {
                if (static_cast<int>(messages.size()) < required) {
                    continue;
                }

                PrepareCertificate cert;
                cert.set_seq_num(seq);
                cert.set_sender_id(this->node_id);
                Ballot* cert_ballot = cert.mutable_b();
                if (!messages.empty()) {
                    cert_ballot->set_ballot_num(messages.front().b().ballot_num());
                    cert_ballot->set_sender_id(messages.front().b().sender_id());
                } else {
                    cert_ballot->set_ballot_num(this->ballot_num);
                    cert_ballot->set_sender_id(this->node_id);
                }

                for (const auto& prepare_msg : messages) {
                    Prepare* added = cert.add_prepares();
                    added->CopyFrom(prepare_msg);
                    added->set_digest(prepare_msg.digest());
                    added->set_signature(prepare_msg.signature());
                }

                certificates.push_back(cert);
            }
        }

        return certificates;
    }

    std::vector<CommitCertificate> collect_commit_certificates_for_view_change() {
        std::vector<CommitCertificate> certificates;
        const int executed_upto = this->execution_index;
        const int f = std::max(1, (this->nodes_num - 1) / 3);
        const int required = std::max(1, 2 * f);

        std::lock_guard<std::mutex> lock(this->pbft_commit_mutex);
        for (const auto& [seq, digest_map] : this->pbft_commit_messages) {
            if (seq <= executed_upto) {
                continue;
            }
            for (const auto& [digest, messages] : digest_map) {
                if (static_cast<int>(messages.size()) < required) {
                    continue;
                }

                CommitCertificate cert;
                cert.set_seq_num(seq);
                cert.set_sender_id(this->node_id);
                Ballot* cert_ballot = cert.mutable_b();
                if (!messages.empty()) {
                    cert_ballot->set_ballot_num(messages.front().b().ballot_num());
                    cert_ballot->set_sender_id(messages.front().b().sender_id());
                } else {
                    cert_ballot->set_ballot_num(this->ballot_num);
                    cert_ballot->set_sender_id(this->node_id);
                }

                for (const auto& commit_msg : messages) {
                    CommitLeader* added = cert.add_commits();
                    added->CopyFrom(commit_msg);
                    added->set_digest(commit_msg.digest());
                    added->set_signature(commit_msg.signature());
                }

                certificates.push_back(cert);
            }
        }

        return certificates;
    }

    ViewChange build_view_change_message(int target_ballot) {
        ViewChange message;
        Ballot* ballot = message.mutable_b();
        ballot->set_ballot_num(target_ballot);
        ballot->set_sender_id(this->node_id);
        // cout<<"Node "<<this->node_id<<" Building View Change Message for Ballot :"<<target_ballot<<endl;
        auto prepared = this->collect_prepared_certificates_for_view_change();
        for (const auto& cert : prepared) {
            PrepareCertificate* added = message.add_prepared();
            *added = cert;
        }
        
        auto committed = this->collect_commit_certificates_for_view_change();
        for (const auto& cert : committed) {
            CommitCertificate* added = message.add_committed();
            *added = cert;
        }
        // cout<<"Node "<<this->node_id<<" Built View Change Message for Ballot :"<<target_ballot<<endl;

        if (this->sign) {
            auto assign_bogus_signature = [this]() {
                return this->sign_payload("view-change");
            };
            for (int cert_idx = 0; cert_idx < message.prepared_size(); ++cert_idx) {
                auto* cert_ptr = message.mutable_prepared(cert_idx);
                for (int prep_idx = 0; prep_idx < cert_ptr->prepares_size(); ++prep_idx) {
                    cert_ptr->mutable_prepares(prep_idx)->set_signature(assign_bogus_signature());
                }
            }
            for (int cert_idx = 0; cert_idx < message.committed_size(); ++cert_idx) {
                auto* cert_ptr = message.mutable_committed(cert_idx);
                for (int commit_idx = 0; commit_idx < cert_ptr->commits_size(); ++commit_idx) {
                    cert_ptr->mutable_commits(commit_idx)->set_signature(assign_bogus_signature());
                }
            }
        }

        return message;
    }

    void send_view_change(int receiver_id, const ViewChange& req) {
        if (this->stop_process) return;
        if (receiver_id == this->node_id) return;

        auto it = this->stubs.find(receiver_id);
        if (it == this->stubs.end()) {
            std::cerr << "Node " << this->node_id << " has no stub for view change to peer " << receiver_id << std::endl;
            return;
        }

        if(this->time_delay)this->delay_timing_attack();

        auto call = std::make_shared<AsyncViewChangeCall>();
        call->context = std::make_shared<ClientContext>();
        call->completion_queue = std::make_shared<CompletionQueue>();
        call->reply = std::make_shared<Ack>();
        call->status = std::make_shared<Status>();
        call->request = std::make_shared<ViewChange>(req);
        call->receiver_id = receiver_id;
    call->responder = it->second->AsyncSendViewChange(call->context.get(), *call->request, call->completion_queue.get());
        call->responder->Finish(call->reply.get(), call->status.get(), (void*)nullptr);

    }

    void broadcast_view_change(const ViewChange& message) {
        for (auto& [peer_id, stub] : this->stubs) {
            if (peer_id == this->node_id || (this->dark && this->dark_set.find(peer_id) != this->dark_set.end())) {
                continue;
            }
            std::thread(&Node::send_view_change, this, peer_id, message).detach();
        }
    }

    void maybe_start_election_from_view_change(int target_ballot) {
        if (this->stop_process) {
            return;
        }

        std::lock_guard<std::mutex> lock(this->vote_mutex);
        if (this->running_election) {
            return;
        }

        if (target_ballot <= this->ballot_num) {
            target_ballot = this->ballot_num + 1;
        }

        this->ballot_num = target_ballot - 1;
        // this->running_election = true;
        // std::thread(&Node::start_election, this).detach();
    }

    void trigger_view_change_due_to_timeout() {

        std::lock_guard<std::mutex> lock(this->view_change_mutex);
        // cout<<"Node "<<this->node_id<<" Stopping Timeout"<<endl;
        // if(this->timer_stopped==false){
        //     // lock_guard<mutex> lock(time_mutex);
        //     this->stop_timer();
        // }
        // cout<<"Node "<<this->node_id<<" Timer Stopped"<<endl;
        // this->view_change_by_sender.clear();
        // this->new_view_broadcasted_ballots.clear();
        int target_ballot = max(this->active_view_change_ballot, this->ballot_num)+1;
        ViewChange message = this->build_view_change_message(target_ballot);
        this->active_view_change_ballot=target_ballot;

        {
            // std::lock_guard<std::mutex> lock(this->view_change_mutex);
            // if (this->active_view_change_ballot != target_ballot) {
            //     this->view_change_by_sender.clear();
            //     this->view_change_broadcasted = false;
            //     this->new_view_broadcasted_ballots.erase(target_ballot);
            // }
            this->view_change_by_sender[this->node_id] = message;
            this->view_change_broadcasted = true;
            this->view_change_local_timer_triggered = true;
            // this->active_view_change_ballot = target_ballot;
        }
        
        std::cout << "Node " << this->node_id << " broadcasting view change for ballot " << target_ballot << std::endl;
        this->block_msg_vc=true;
        this->broadcast_view_change(message);
        
        if(this->leader_for_ballot(target_ballot)==this->node_id){
            int count_for_ballot = 0;
            for (auto vc_it = this->view_change_by_sender.begin(); vc_it != this->view_change_by_sender.end(); vc_it++) {
                // if (vc_it->second.b().ballot_num() != ballot) {
                    //     vc_it = this->view_change_by_sender.erase(vc_it);
                    // } else {
                        //     ++count_for_ballot;
                        //     ++vc_it;
                        // }
                if (vc_it->second.b().ballot_num() == target_ballot) count_for_ballot++;
            }
            if(count_for_ballot>=((2*this->nodes_num)/3)+1){
                this->send_new_view_after_timeout(target_ballot);
            }
        }
        // this->maybe_start_election_from_view_change(target_ballot);
    }

    void reset_view_change_state() {
        std::lock_guard<std::mutex> lock(this->view_change_mutex);
        this->view_change_by_sender.clear();
        this->view_change_local_timer_triggered = false;
        this->view_change_broadcasted = false;
        this->active_view_change_ballot = -1;
        this->new_view_broadcasted_ballots.clear();
    }

    NewView build_new_view_message(int target_ballot, const std::vector<ViewChange>& view_changes) {
        using SeqDigestKey = std::pair<int, std::string>;

        NewView message;
        Ballot* ballot = message.mutable_b();
        ballot->set_ballot_num(target_ballot);
        ballot->set_sender_id(this->node_id);

        std::map<SeqDigestKey, PrepareCertificate> prepared_map;
        std::map<SeqDigestKey, std::set<int>> prepared_senders;
        std::map<SeqDigestKey, CommitCertificate> commit_map;
        std::map<SeqDigestKey, std::set<int>> commit_senders;

        for (const auto& vc : view_changes) {
            ViewChange* added_vc = message.add_view_changes();
            *added_vc = vc;

            for (const auto& cert : vc.prepared()) {
                if (cert.prepares_size() == 0) {
                    continue;
                }
                std::string digest_key = cert.prepares(0).digest();
                SeqDigestKey key{cert.seq_num(), digest_key};

                auto [it, inserted] = prepared_map.emplace(key, PrepareCertificate());
                if (inserted || it->second.prepares_size() == 0) {
                    it->second = cert;
                    std::set<int> senders;
                    for (const auto& prepare_msg : cert.prepares()) {
                        senders.insert(prepare_msg.b().sender_id());
                    }
                    prepared_senders[key] = std::move(senders);
                } else {
                    auto& stored = it->second;
                    auto& senders = prepared_senders[key];
                    for (const auto& prepare_msg : cert.prepares()) {
                        if (senders.insert(prepare_msg.b().sender_id()).second) {
                            Prepare* added = stored.add_prepares();
                            *added = prepare_msg;
                        }
                    }
                }
            }

            for (const auto& cert : vc.committed()) {
                if (cert.commits_size() == 0) {
                    continue;
                }
                std::string digest_key = cert.commits(0).digest();
                SeqDigestKey key{cert.seq_num(), digest_key};

                auto [it, inserted] = commit_map.emplace(key, CommitCertificate());
                if (inserted || it->second.commits_size() == 0) {
                    it->second = cert;
                    std::set<int> senders;
                    for (const auto& commit_msg : cert.commits()) {
                        senders.insert(commit_msg.sender_id());
                    }
                    commit_senders[key] = std::move(senders);
                } else {
                    auto& stored = it->second;
                    auto& senders = commit_senders[key];
                    for (const auto& commit_msg : cert.commits()) {
                        if (senders.insert(commit_msg.sender_id()).second) {
                            CommitLeader* added = stored.add_commits();
                            *added = commit_msg;
                        }
                    }
                }
            }
        }

        for (auto& [key, cert] : prepared_map) {
            PrepareCertificate* added = message.add_prepared();
            *added = cert;
        }

        for (auto& [key, cert] : commit_map) {
            CommitCertificate* added = message.add_committed();
            *added = cert;
        }

        return message;
    }

    void send_new_view(int receiver_id, const NewView& req) {
        if (this->stop_process) return;
        if (receiver_id == this->node_id) return;

        auto it = this->stubs.find(receiver_id);
        if (it == this->stubs.end()) {
            std::cerr << "Node " << this->node_id << " has no stub for new view to peer " << receiver_id << std::endl;
            return;
        }

        if(this->time_delay)this->delay_timing_attack();

        auto call = std::make_shared<AsyncNewViewCall>();
        call->context = std::make_shared<ClientContext>();
        call->completion_queue = std::make_shared<CompletionQueue>();
        call->reply = std::make_shared<Ack>();
        call->status = std::make_shared<Status>();
        call->request = std::make_shared<NewView>(req);
        call->receiver_id = receiver_id;
    call->responder = it->second->AsyncSendNewView(call->context.get(), *call->request, call->completion_queue.get());
        call->responder->Finish(call->reply.get(), call->status.get(), call.get());

        std::thread([this, call]() {
            void* tag = nullptr;
            bool ok = false;
            if (call->completion_queue->Next(&tag, &ok)) {
                if (!ok || tag != static_cast<void*>(call.get()) || !call->status->ok()) {
                    std::cerr << "Node " << this->node_id
                              << " async new view to peer " << call->receiver_id
                              << " failed: " << call->status->error_message() << std::endl;
                }
            }
            call->completion_queue->Shutdown();
        }).detach();
    }

    void broadcast_new_view(const NewView& message) {
        for (auto& [peer_id, stub] : this->stubs) {
            if (this->crash||peer_id == this->node_id || (this->dark && this->dark_set.find(peer_id) != this->dark_set.end())) {
                continue;
            }
            std::thread(&Node::send_new_view, this, peer_id, message).detach();
        }
    }

    void integrate_prepare_certificate(const PrepareCertificate& cert) {
        if (cert.prepares_size() == 0) {
            return;
        }

        const int seq = cert.seq_num();
        const std::string digest_key = cert.prepares(0).digest();

        std::lock_guard<std::mutex> lock(this->pbft_prepare_mutex);
        auto& sender_set = this->pbft_prepare_senders[seq][digest_key];
        auto& messages = this->pbft_prepare_messages[seq][digest_key];
        for (const auto& prepare_msg : cert.prepares()) {
            if (sender_set.insert(prepare_msg.b().sender_id()).second) {
                messages.push_back(prepare_msg);
            }
        }
        auto& vote_set = this->pbft_prepare_votes[seq];
        vote_set.insert(sender_set.begin(), sender_set.end());
    }

    void integrate_commit_certificate(const CommitCertificate& cert) {
        if (cert.commits_size() == 0) {
            return;
        }

        const int seq = cert.seq_num();
        const std::string digest_key = cert.commits(0).digest();

        std::lock_guard<std::mutex> lock(this->pbft_commit_mutex);
        auto& sender_set = this->pbft_commit_senders[seq][digest_key];
        auto& messages = this->pbft_commit_messages[seq][digest_key];
        for (const auto& commit_msg : cert.commits()) {
            if (sender_set.insert(commit_msg.sender_id()).second) {
                messages.push_back(commit_msg);
            }
        }
    }

    bool apply_new_view(const NewView& message) {
        if (!this->validate_new_view(message)) {
            std::cerr << "Node " << this->node_id
                      << " rejected NewView for ballot " << message.b().ballot_num()
                      << " due to invalid certificates" << std::endl;
            return false;
        }

        const int ballot = message.b().ballot_num();
        const int leader_id = message.b().sender_id();

        {
            std::lock_guard<std::mutex> lock(this->view_change_mutex);
            this->active_view_change_ballot = -1;
            this->view_change_by_sender.clear();
            for (const auto& vc : message.view_changes()) {
                this->view_change_by_sender[vc.b().sender_id()] = vc;
            }
            this->view_change_broadcasted = true;
            this->view_change_local_timer_triggered = true;
            this->new_view_broadcasted_ballots.insert(ballot);
        }

    this->ballot_num = std::max(this->ballot_num, ballot);
    this->cur_leader = leader_id;
    this->running_election = false;
    this->stop_election = false;
    this->is_leader = (this->cur_leader == this->node_id);

        for (const auto& cert : message.prepared()) {
            this->integrate_prepare_certificate(cert);
        }

        for (const auto& cert : message.committed()) {
            this->integrate_commit_certificate(cert);
        }

        this->rebuild_log_from_new_view(message);
        this->block_msg_vc=false;
        // this->reset_timer();
        if(!this->is_leader){

            std::cout << "Node " << this->node_id << " accepted NewView for ballot "
                  << this->ballot_num << " from leader " << this->cur_leader<< std::endl;
            this->restart_consensus_after_new_view();
        }
        return true;
    }

    void notify_client_of_execution(const Log& log_entry, bool success) {
        if (log_entry.unique == -1) {
            return;
        }

        if (!this->exec_tns_unique.insert(log_entry.unique).second) {
            return;
        }

        int client_id = -1;
        auto client_it = this->tns_to_client.find(log_entry.unique);
        if (client_it != this->tns_to_client.end()) {
            client_id = client_it->second;
        }

        if (client_id == -1) {
            return;
        }

        auto stub_it = this->client_stubs.find(client_id);
        if (stub_it == this->client_stubs.end()) {
            return;
        }

        TransCompleted request;
        request.mutable_b()->set_ballot_num(this->ballot_num);
        request.mutable_b()->set_sender_id(this->node_id);
        request.set_c1(client_id);
        request.set_unique(log_entry.unique);
        request.set_sender_id(this->node_id);
        request.set_r(success);

        ClientContext context;
        CompletionQueue cq;
        Ack reply;
        Status status;
        auto rpc = stub_it->second->AsyncSendTransCompleted(&context, request, &cq);
        rpc->Finish(&reply, &status, (void*)nullptr);
    }

    std::string compute_transaction_digest(const Transaction& txn) const {
        std::string serialized;
        if (!txn.SerializeToString(&serialized)) {
            throw std::runtime_error("Failed to serialize transaction for hashing");
        }

        CryptoPP::SHA256 hash;
        std::string digest;
        CryptoPP::StringSource(serialized, true,
            new CryptoPP::HashFilter(hash,
                new CryptoPP::StringSink(digest)));
        return digest;
    }

    std::string digest_to_hex(const std::string& digest) const {
        std::string encoded;
        CryptoPP::StringSource(digest, true,
            new CryptoPP::HexEncoder(
                new CryptoPP::StringSink(encoded), false));
        return encoded;
    }

    std::string build_prepare_payload(int ballot, int seq, const std::string& digest) const {
        return std::to_string(ballot) + "|" + std::to_string(seq) + "|" + digest;
    }

    std::string sign_payload(const std::string& payload) {
        if (this->sign) {
            CryptoPP::AutoSeededRandomPool rng;
            std::string bogus_signature(32, '\0');
            rng.GenerateBlock(reinterpret_cast<CryptoPP::byte*>(&bogus_signature[0]), bogus_signature.size());
            return bogus_signature;
        }
        CryptoPP::AutoSeededRandomPool rng;
        CryptoPP::RSASSA_PKCS1v15_SHA_Signer signer(this->private_key);
        std::string signature;
        CryptoPP::StringSource(payload, true,
            new CryptoPP::SignerFilter(rng, signer,
                new CryptoPP::StringSink(signature)));
        return signature;
    }

    bool verify_peer_signature(int peer_id, const std::string& payload, const std::string& signature) {
        if (peer_id == this->node_id) {
            CryptoPP::RSASSA_PKCS1v15_SHA_Verifier verifier(this->public_key);
            return verifier.VerifyMessage(reinterpret_cast<const unsigned char*>(payload.data()), payload.size(),
                                          reinterpret_cast<const unsigned char*>(signature.data()), signature.size());
        }

        auto peer_key = this->get_peer_public_key(peer_id);
        if (!peer_key.has_value()) {
            std::cerr << "Node " << this->node_id << " missing public key for peer " << peer_id << std::endl;
            return false;
        }

    CryptoPP::RSASSA_PKCS1v15_SHA_Verifier verifier(peer_key.value());
    return verifier.VerifyMessage(reinterpret_cast<const unsigned char*>(payload.data()), payload.size(),
                      reinterpret_cast<const unsigned char*>(signature.data()), signature.size());
    }

    void merge_logs_from_ack(const Acknowledge& reply) {
        std::unique_lock<std::mutex> log_lock(this->logs_mutex);

        int cur_seq = 1;
        int prev_size = static_cast<int>(this->logs.size());
        int target_size = std::max(static_cast<int>(this->logs.size()), static_cast<int>(reply.log().size()));
        this->logs.resize(target_size, Log());

        for (const auto& peer_log : reply.log()) {
            // If sequence in the peer log doesn't align with our expected current seq, skip
            if (cur_seq != peer_log.seq_num()) {
                cur_seq++;
                continue;
            }

            // If already executed or committed, skip
            if (this->execution_index >= cur_seq || this->commit_set.find(cur_seq) != this->commit_set.end()) {
                cur_seq++;
                continue;
            }

            // If peer has no transaction at this seq (unique == -1), write a noop
            if (peer_log.tns().unique() == -1) {
                if (this->logs[cur_seq - 1].seq_num != -1) {
                    this->tns_log.erase(this->logs[cur_seq - 1].unique);
                    this->tns_to_client.erase(this->logs[cur_seq - 1].unique);
                }
                this->logs[cur_seq - 1] = Log(std::make_pair(this->ballot_num, this->node_id),
                                              cur_seq,
                                              std::make_pair(std::make_pair(std::string("noop"), std::string("noop")), 0),
                                              -1);
                cur_seq++;
                continue;
            }

            // If peer has entries beyond our previous size, adopt them
            if (cur_seq > prev_size) {
                if (peer_log.tns().unique() != -1) {
                    if (this->logs[cur_seq - 1].seq_num != -1) {
                        this->tns_log.erase(this->logs[cur_seq - 1].unique);
                        this->tns_to_client.erase(this->logs[cur_seq - 1].unique);
                    }
                    this->tns_log.insert(peer_log.tns().unique());
                    if (!peer_log.tns().c().c1().empty()) {
                        this->tns_to_client[peer_log.tns().unique()] = peer_log.tns().c().c1()[0] - 'A' + 1;
                    }
                    this->logs[peer_log.seq_num() - 1] = Log(std::make_pair(this->ballot_num, this->node_id),
                                                             cur_seq,
                                                             std::make_pair(std::make_pair(peer_log.tns().c().c1(), peer_log.tns().c().c2()), peer_log.tns().amt()),
                                                             peer_log.tns().unique());
                } else {
                    if (this->logs[cur_seq - 1].seq_num != -1) {
                        this->tns_log.erase(this->logs[cur_seq - 1].unique);
                        this->tns_to_client.erase(this->logs[cur_seq - 1].unique);
                    }
                    this->logs[peer_log.seq_num() - 1] = Log(std::make_pair(this->ballot_num, this->node_id),
                                                             cur_seq,
                                                             std::make_pair(std::make_pair(std::string("noop"), std::string("noop")), 0),
                                                             -1);
                }
                cur_seq++;
                continue;
            }

            // Otherwise consider replacing our entry if the peer's has a higher ballot or ours is a noop
            if ((this->logs[cur_seq - 1].tns.first.first == "noop" && peer_log.tns().c().c1() != "noop") ||
                this->logs[cur_seq - 1].b.first < peer_log.b().ballot_num()) {

                if (this->logs[cur_seq - 1].seq_num != -1) {
                    this->tns_log.erase(this->logs[cur_seq - 1].unique);
                }
                if (peer_log.tns().unique() != -1) {
                    this->tns_log.insert(peer_log.tns().unique());
                }

                this->logs[cur_seq - 1] = Log(std::make_pair(peer_log.b().ballot_num(), peer_log.b().sender_id()),
                                              cur_seq,
                                              std::make_pair(std::make_pair(peer_log.tns().c().c1(), peer_log.tns().c().c2()), peer_log.tns().amt()),
                                              peer_log.tns().unique());
            }
            cur_seq++;

        }

        for (const auto& log_entry : this->logs) {
            if (log_entry.seq_num == -1 || log_entry.tns.first.first == "noop") {
                continue;
            }
            Transaction cached_txn;
            Clients* cached_clients = cached_txn.mutable_c();
            cached_clients->set_c1(log_entry.tns.first.first);
            cached_clients->set_c2(log_entry.tns.first.second);
            cached_txn.set_amt(log_entry.tns.second);
            cached_txn.set_unique(log_entry.unique);
            std::string digest = this->compute_transaction_digest(cached_txn);
            this->remember_transaction_digest(digest, cached_txn);
        }

        log_lock.unlock();
        std::lock_guard<std::mutex> seq_lock(this->seq_num_mutex);
        this->seq_num = std::max(this->seq_num, cur_seq);
    }

    void remember_transaction_digest(const std::string& digest, const Transaction& txn) {
        if (digest.empty()) {
            return;
        }
        std::lock_guard<std::mutex> lock(this->digest_map_mutex);
        if (this->digest_to_transaction.find(digest) != this->digest_to_transaction.end()) {
            return;
        }
        Transaction copy;
        copy.CopyFrom(txn);
        this->digest_to_transaction.emplace(digest, std::move(copy));
    }

    std::optional<Transaction> lookup_transaction_by_digest(const std::string& digest) const {
        std::lock_guard<std::mutex> lock(this->digest_map_mutex);
        auto it = this->digest_to_transaction.find(digest);
        if (it == this->digest_to_transaction.end()) {
            return std::nullopt;
        }
        Transaction copy;
        copy.CopyFrom(it->second);
        return copy;
    }

    void accumulate_prepare_certificate(int seq, const PrepareCertificate& cert, std::unordered_map<int, SeqAggregateInfo>& per_seq) {
        if (seq <= 0 || cert.prepares_size() == 0) {
            return;
        }
        auto& seq_info = per_seq[seq];
        const std::string& digest = cert.prepares(0).digest();
        auto& digest_info = seq_info.digest_map[digest];
        digest_info.has_prepare = true;
        for (const auto& prepare_msg : cert.prepares()) {
            digest_info.unique_senders.insert(prepare_msg.b().sender_id());
            int ballot = prepare_msg.b().ballot_num();
            if (ballot > digest_info.highest_ballot) {
                digest_info.highest_ballot = ballot;
                digest_info.highest_sender = prepare_msg.b().sender_id();
            }
        }
    }

    void accumulate_commit_certificate(int seq, const CommitCertificate& cert, std::unordered_map<int, SeqAggregateInfo>& per_seq) {
        if (seq <= 0 || cert.commits_size() == 0) {
            return;
        }
        auto& seq_info = per_seq[seq];
        const std::string& digest = cert.commits(0).digest();
        auto& digest_info = seq_info.digest_map[digest];
        digest_info.has_commit = true;
        for (const auto& commit_msg : cert.commits()) {
            digest_info.unique_senders.insert(commit_msg.sender_id());
            int ballot = commit_msg.b().ballot_num();
            if (ballot > digest_info.highest_ballot) {
                digest_info.highest_ballot = ballot;
                digest_info.highest_sender = commit_msg.sender_id();
            }
        }
    }

    void aggregate_view_change_data(const std::vector<ViewChange>& view_changes,
                                    std::unordered_map<int, SeqAggregateInfo>& per_seq,
                                    int& max_seq) {
        for (const auto& vc : view_changes) {
            for (const auto& cert : vc.prepared()) {
                int seq = cert.seq_num();
                this->accumulate_prepare_certificate(seq, cert, per_seq);
                if (seq > max_seq) {
                    max_seq = seq;
                }
            }
            for (const auto& cert : vc.committed()) {
                int seq = cert.seq_num();
                this->accumulate_commit_certificate(seq, cert, per_seq);
                if (seq > max_seq) {
                    max_seq = seq;
                }
            }
        }
    }

    void aggregate_certificates(const std::vector<PrepareCertificate>& prepared,
                                const std::vector<CommitCertificate>& committed,
                                std::unordered_map<int, SeqAggregateInfo>& per_seq,
                                int& max_seq) {
        for (const auto& cert : prepared) {
            int seq = cert.seq_num();
            this->accumulate_prepare_certificate(seq, cert, per_seq);
            if (seq > max_seq) {
                max_seq = seq;
            }
        }
        for (const auto& cert : committed) {
            int seq = cert.seq_num();
            this->accumulate_commit_certificate(seq, cert, per_seq);
            if (seq > max_seq) {
                max_seq = seq;
            }
        }
    }

    std::vector<ReconstructedEntry> construct_reconstructed_log(
        const std::unordered_map<int, SeqAggregateInfo>& per_seq,
        int max_seq) {
        if (max_seq <= 0) {
            return {};
        }

        std::vector<ReconstructedEntry> decisions(static_cast<size_t>(max_seq));
        int f = std::max(1, (this->nodes_num - 1) / 3);
        int majority_threshold = std::max(1, 2 * f + 1);

        for (int seq = 1; seq <= max_seq; ++seq) {
            ReconstructedEntry decision;
            auto it = per_seq.find(seq);
            if (it != per_seq.end()) {
                const auto& digest_map = it->second.digest_map;
                const DigestAggregateInfo* best_info = nullptr;
                std::string best_digest;
                int best_support = -1;
                int best_ballot = -1;

                for (const auto& [digest, info] : digest_map) {
                    int support = static_cast<int>(info.unique_senders.size());
                    int ballot = info.highest_ballot;
                    if (support > best_support || (support == best_support && ballot > best_ballot)) {
                        best_support = support;
                        best_ballot = ballot;
                        best_digest = digest;
                        best_info = &info;
                    }
                }

                if (best_info && (best_support >= majority_threshold || best_info->highest_ballot >= 0)) {
                    auto maybe_txn = this->lookup_transaction_by_digest(best_digest);
                    if (maybe_txn.has_value()) {
                        Log entry({best_info->highest_ballot >= 0 ? best_info->highest_ballot : this->ballot_num,
                                   best_info->highest_sender >= 0 ? best_info->highest_sender : this->node_id},
                                  seq,
                                  {{maybe_txn->c().c1(), maybe_txn->c().c2()}, maybe_txn->amt()},
                                  maybe_txn->unique());
                        decision.log_entry = entry;
                        decision.marked_committed = best_info->has_commit;
                        decision.marked_prepared = best_info->has_prepare || best_info->has_commit;
                    } else {
                        decision.log_entry = Log({this->ballot_num, this->node_id}, seq,
                                                 {{"noop","noop"}, 0}, -1);
                    }
                } else {
                    decision.log_entry = Log({this->ballot_num, this->node_id}, seq,
                                             {{"noop","noop"}, 0}, -1);
                }
            } else {
                decision.log_entry = Log({this->ballot_num, this->node_id}, seq,
                                         {{"noop","noop"}, 0}, -1);
            }

            decisions[static_cast<size_t>(seq - 1)] = decision;
        }

        return decisions;
    }

    void apply_reconstructed_entries(const std::vector<ReconstructedEntry>& entries) {
        if (entries.empty()) {
            return;
        }

        const int prior_execution = this->execution_index;

    std::vector<Log> new_logs(entries.size());
    std::set<int> new_tns_log;
    std::map<int, int> new_tns_to_client;
    std::set<int> new_commit_set;
    std::set<int> new_commit_tns_unique;
    std::vector<std::pair<int, TxStatus>> status_assignments;
    std::vector<int> executed_uniques;
    executed_uniques.reserve(static_cast<size_t>(std::min(prior_execution, static_cast<int>(entries.size()))));

        for (size_t idx = 0; idx < entries.size(); ++idx) {
            Log log = entries[idx].log_entry;
            int seq = static_cast<int>(idx) + 1;
            log.seq_num = seq;
            if (log.b.first == -1) {
                log.b.first = this->ballot_num;
            }
            if (log.b.second == -1) {
                log.b.second = this->node_id;
            }

            if (log.unique != -1 && log.tns.first.first != "noop") {
                new_tns_log.insert(log.unique);
                if (!log.tns.first.first.empty()) {
                    new_tns_to_client[log.unique] = log.tns.first.first[0] - 'A' + 1;
                }
            }

            if (entries[idx].marked_committed) {
                new_commit_set.insert(log.seq_num);
                if (log.unique != -1) {
                    new_commit_tns_unique.insert(log.unique);
                }
                status_assignments.emplace_back(log.seq_num, TxStatus::Committed);
            } else if (entries[idx].marked_prepared) {
                status_assignments.emplace_back(log.seq_num, TxStatus::Prepared);
            }

            new_logs[idx] = log;

            if (seq <= prior_execution) {
                executed_uniques.push_back(log.unique);
            }
        }

        {
            std::lock_guard<std::mutex> log_guard(this->logs_mutex);
            this->logs = std::move(new_logs);
            this->tns_log = std::move(new_tns_log);
            this->tns_to_client = std::move(new_tns_to_client);
        }

        {
            std::lock_guard<std::mutex> commit_guard(this->commit_set_mutex);
            this->commit_set = std::move(new_commit_set);
            this->commit_tns_unique = std::move(new_commit_tns_unique);

            int tracked_execution = std::min(prior_execution, static_cast<int>(executed_uniques.size()));
            for (int seq = 1; seq <= tracked_execution; ++seq) {
                this->commit_set.insert(seq);
                int unique_val = executed_uniques[seq - 1];
                if (unique_val != -1) {
                    this->commit_tns_unique.insert(unique_val);
                }
            }
        }

        {
            std::lock_guard<std::mutex> seq_guard(this->seq_num_mutex);
            this->seq_num = static_cast<int>(entries.size()) + 1;
        }

        this->execution_index = std::min(prior_execution, static_cast<int>(entries.size()));

        this->clear_sequence_status();
        for (const auto& [seq, status] : status_assignments) {
            this->update_sequence_status(seq, status);
        }
        for (int seq = 1; seq <= this->execution_index; ++seq) {
            this->update_sequence_status(seq, TxStatus::Executed);
        }

        for (const auto& entry : entries) {
            const Log& log = entry.log_entry;
            if (log.seq_num <= 0 || log.unique == -1 || log.tns.first.first == "noop") {
                continue;
            }
            Transaction txn;
            Clients* clients = txn.mutable_c();
            clients->set_c1(log.tns.first.first);
            clients->set_c2(log.tns.first.second);
            txn.set_amt(log.tns.second);
            txn.set_unique(log.unique);
            std::string digest = this->compute_transaction_digest(txn);
            this->remember_transaction_digest(digest, txn);
        }
    }

    void clear_consensus_tracking() {
        {
            std::lock_guard<std::mutex> prep_lock(this->pbft_prepare_mutex);
            this->pbft_prepare_votes.clear();
            this->pbft_prepare_messages.clear();
            this->pbft_prepare_senders.clear();
            this->pbft_prepare_certificates_sent.clear();
        }
        {
            std::lock_guard<std::mutex> commit_lock(this->pbft_commit_mutex);
            this->pbft_commit_messages.clear();
            this->pbft_commit_senders.clear();
            this->pbft_commit_certificates_sent.clear();
        }
    }

    void restart_consensus_after_new_view() {
        if (this->stop_process) {
            return;
        }

        this->clear_consensus_tracking();

        const bool is_new_leader = this->is_leader;

        std::vector<Log> pending_entries;
        {
            std::lock_guard<std::mutex> log_guard(this->logs_mutex);
            pending_entries.reserve(this->logs.size());
            for (auto& entry : this->logs) {
                if (entry.seq_num == -1) {
                    continue;
                }
                if (is_new_leader) {
                    entry.b.first = this->ballot_num;
                    entry.b.second = this->node_id;
                }
                pending_entries.push_back(entry);
            }
        }

        if (pending_entries.empty()) {
            return;
        }

        int executed_upto = 0;
        std::set<int> committed_snapshot;
        {
            std::lock_guard<std::mutex> commit_guard(this->commit_set_mutex);
            committed_snapshot = this->commit_set;
            executed_upto = this->execution_index;
        }

        const int leader_id = this->cur_leader;

        for (const auto& entry : pending_entries) {
            if (entry.seq_num <= 0) {
                continue;
            }
            if (entry.seq_num <= executed_upto) {
                continue;
            }
            if (committed_snapshot.count(entry.seq_num) > 0) {
                continue;
            }

            Transaction txn;
            Clients* clients = txn.mutable_c();
            clients->set_c1(entry.tns.first.first);
            clients->set_c2(entry.tns.first.second);
            txn.set_amt(entry.tns.second);
            txn.set_unique(entry.unique);

            std::string digest;
            try {
                digest = this->compute_transaction_digest(txn);
            } catch (const std::exception& ex) {
                std::cerr << "Node " << this->node_id
                          << " unable to restart consensus for seq " << entry.seq_num
                          << ": " << ex.what() << std::endl;
                continue;
            }

            this->remember_transaction_digest(digest, txn);
            this->update_sequence_status(entry.seq_num, TxStatus::PrePrepared);

            if (is_new_leader) {
                std::lock_guard<std::mutex> prep_guard(this->pbft_prepare_mutex);
                this->pbft_prepare_votes[entry.seq_num].insert(this->node_id);
                continue;
            }

            bool inserted = false;
            {
                std::lock_guard<std::mutex> prep_guard(this->pbft_prepare_mutex);
                auto& vote_set = this->pbft_prepare_votes[entry.seq_num];
                inserted = vote_set.insert(this->node_id).second;
            }
            if (!inserted) {
                continue;
            }

            if (leader_id <= 0 || leader_id == this->node_id) {
                continue;
            }
            if (this->crash) {
                continue;
            }
            if (this->dark && this->dark_set.find(leader_id) != this->dark_set.end()) {
                continue;
            }

            Prepare prepare_msg;
            prepare_msg.set_type("PBFT_PREPARE");
            Ballot* prepare_ballot = prepare_msg.mutable_b();
            int ballot_to_use = entry.b.first > 0 ? entry.b.first : this->ballot_num;
            prepare_ballot->set_ballot_num(ballot_to_use);
            prepare_ballot->set_sender_id(this->node_id);
            prepare_msg.set_seq_num(entry.seq_num);
            prepare_msg.set_digest(digest);
            const std::string payload = this->build_prepare_payload(ballot_to_use,
                                                                    entry.seq_num,
                                                                    digest);
            prepare_msg.set_signature(this->sign_payload(payload));

            std::thread(&Node::send_prepare_leader, this, leader_id, prepare_msg).detach();
        }
    }

    void rebuild_log_from_view_changes(const std::vector<ViewChange>& view_changes) {
        if (view_changes.empty()) {
            return;
        }

        std::unordered_map<int, SeqAggregateInfo> per_seq;
        int max_seq = 0;
        this->aggregate_view_change_data(view_changes, per_seq, max_seq);
        max_seq = std::max(max_seq, static_cast<int>(this->logs.size()));
        if (max_seq <= 0) {
            return;
        }

        auto reconstructed = this->construct_reconstructed_log(per_seq, max_seq);
        this->apply_reconstructed_entries(reconstructed);
    }

    void rebuild_log_from_new_view(const NewView& message) {
        std::unordered_map<int, SeqAggregateInfo> per_seq;
        int max_seq = 0;

        std::vector<ViewChange> vc_list;
        vc_list.reserve(message.view_changes().size());
        for (const auto& vc : message.view_changes()) {
            vc_list.push_back(vc);
        }
        this->aggregate_view_change_data(vc_list, per_seq, max_seq);

        std::vector<PrepareCertificate> prepared_list;
        prepared_list.reserve(message.prepared().size());
        for (const auto& cert : message.prepared()) {
            prepared_list.push_back(cert);
        }

        std::vector<CommitCertificate> committed_list;
        committed_list.reserve(message.committed().size());
        for (const auto& cert : message.committed()) {
            committed_list.push_back(cert);
        }

        this->aggregate_certificates(prepared_list, committed_list, per_seq, max_seq);
        max_seq = std::max(max_seq, static_cast<int>(this->logs.size()));
        if (max_seq <= 0) {
            return;
        }

        auto reconstructed = this->construct_reconstructed_log(per_seq, max_seq);
        this->apply_reconstructed_entries(reconstructed);
    }

    bool validate_prepare_certificate(const PrepareCertificate& cert) {
        if (cert.prepares_size() == 0) {
            return false;
        }
        const int seq = cert.seq_num();
        for (const auto& prepare_msg : cert.prepares()) {
            if (prepare_msg.seq_num() != seq) {
                return false;
            }
            const std::string payload = this->build_prepare_payload(prepare_msg.b().ballot_num(),
                                                                    prepare_msg.seq_num(),
                                                                    prepare_msg.digest());
            if (!this->verify_peer_signature(prepare_msg.b().sender_id(), payload, prepare_msg.signature())) {
                return false;
            }
        }
        return true;
    }

    bool validate_commit_certificate(const CommitCertificate& cert) {
        if (cert.commits_size() == 0) {
            return false;
        }
        const int seq = cert.seq_num();
        for (const auto& commit_msg : cert.commits()) {
            if (commit_msg.seq_num() != seq) {
                return false;
            }
            std::string digest(commit_msg.digest());
            const std::string payload = this->build_prepare_payload(commit_msg.b().ballot_num(),
                                                                    commit_msg.seq_num(),
                                                                    digest);
            if (!this->verify_peer_signature(commit_msg.sender_id(), payload, commit_msg.signature())) {
                return false;
            }
        }
        return true;
    }

    bool validate_view_change_message(const ViewChange& vc) {
        for (const auto& cert : vc.prepared()) {
            if (!this->validate_prepare_certificate(cert)) {
                return false;
            }
        }
        for (const auto& cert : vc.committed()) {
            if (!this->validate_commit_certificate(cert)) {
                return false;
            }
        }
        return true;
    }

    bool validate_new_view(const NewView& message) {
        for (const auto& vc : message.view_changes()) {
            if (!this->validate_view_change_message(vc)) {
                cout<<"Invalid!"<<endl;
                return false;
            }
        }
        for (const auto& cert : message.prepared()) {
            if (!this->validate_prepare_certificate(cert)) {
                cout<<"Invalid!!"<<endl;
                return false;
            }
        }
        for (const auto& cert : message.committed()) {
            if (!this->validate_commit_certificate(cert)) {
                cout<<"Invalid!!!"<<endl;
                return false;
            }
        }
        return true;
    }

    class AsyncCallBase {
    public:
        virtual void Proceed(bool ok) = 0;
        virtual ~AsyncCallBase() = default;
    };

    class AsyncSendClientTransactionCall : public AsyncCallBase {
    public:
        AsyncSendClientTransactionCall(NodeService::AsyncService* service, ServerCompletionQueue* cq, Node* node)
            : service_(service), cq_(cq), responder_(&ctx_), node_(node), status_(CREATE) {
            Proceed(true);
        }

        void Proceed(bool ok) override {
            if (!ok && status_ != CREATE) {
                delete this;
                return;
            }

            if (status_ == CREATE) {
                status_ = PROCESS;
                service_->RequestSendClientTransaction(&ctx_, &req_, &responder_, cq_, cq_, this);
                return;
            }

            if (status_ == PROCESS) {
                new AsyncSendClientTransactionCall(service_, cq_, node_);

                if (!node_->stop_process) {
                    {
                        std::unique_lock<std::mutex> lock(node_->election);
                        node_->cv_election.wait_for(lock, std::chrono::seconds(5),
                                                    [&]() { return node_->cur_leader != -1; });
                    }

                    std::unique_lock<std::mutex> lock(node_->form_new_log_view_mutex);
                    if (node_->cur_leader == node_->node_id) {
                        if (node_->tns_log.find(req_.unique()) == node_->tns_log.end() &&
                            node_->tns_to_client.find(req_.unique()) == node_->tns_to_client.end()) {
                            node_->tns_to_client[req_.unique()] = req_.c1();
                            node_->tns_log.insert(req_.unique());
                            node_->client_reqs.push(Client_Req(
                                {{req_.tns().c().c1(), req_.tns().c().c2()}, req_.tns().amt()},
                                req_.c1(), static_cast<int>(req_.unique())));
                        } else if (node_->exec_status.find(req_.unique()) != node_->exec_status.end()) {
                            ClientContext context;
                            TransCompleted req;
                            CompletionQueue cq;
                            req.set_c1(req_.c1());
                            req.set_unique(req_.unique());
                            req.set_sender_id(node_->node_id);
                            req.set_r(node_->exec_status[req_.unique()]);
                            Ack reply;
                            Status status;
                            std::unique_ptr<ClientAsyncResponseReader<Ack>> rpc(
                                node_->client_stubs[req_.c1()]->AsyncSendTransCompleted(&context, req, &cq));

                            rpc->Finish(&reply, &status, nullptr);
                        }
                    } else {
                        if (!node_->block_msg_vc && node_->cur_leader != -1 && !node_->is_leader) {
                            node_->forward_to_leader(&req_);
                            if(node_->timer_stopped==true){
                                node_->timer_stopped=false;
                                cout<<"Node "<<node_->node_id<<" restarts timer"<<node_->cur_leader<<endl;
                                node_->reset_timer();
                            }
                        }
                    }
                }

                status_ = FINISH;
                responder_.Finish(reply_, Status::OK, this);
                return;
            }

            delete this;
        }

    private:
        NodeService::AsyncService* service_;
        ServerCompletionQueue* cq_;
        ServerContext ctx_;

        Node* node_;
        ClientTransaction req_;
        TransAck reply_;
        ServerAsyncResponseWriter<TransAck> responder_;
        
        enum CallStatus { CREATE, PROCESS, FINISH };
        CallStatus status_;  // The current serving state.
    };

    class AsyncClientReadCall : public AsyncCallBase {
    public:
        AsyncClientReadCall(NodeService::AsyncService* service, ServerCompletionQueue* cq, Node* node)
            : service_(service), cq_(cq), responder_(&ctx_), node_(node), status_(CREATE) {
            Proceed(true);
        }

        void Proceed(bool ok) override {
            if (!ok && status_ != CREATE) {
                delete this;
                return;
            }

            if (status_ == CREATE) {
                status_ = PROCESS;
                service_->RequestSendClientRead(&ctx_, &req_, &responder_, cq_, cq_, this);
                return;
            }

            if (status_ == PROCESS) {
                new AsyncClientReadCall(service_, cq_, node_);

                ClientReadReply reply;
                auto* entry = reply.add_entries();
                entry->set_sender_id(node_->node_id);
                entry->set_key(req_.key());
                entry->set_found(false);
                entry->set_amount(0);

                if (node_->stop_process||node_->crash) return;
                else{
                    std::lock_guard<std::mutex> guard(node_->commit_set_mutex);
                    auto it = node_->db.find(req_.key());
                    if (it != node_->db.end()) {
                        entry->set_found(true);
                        entry->set_amount(it->second);
                    }
                }

                status_ = FINISH;
                responder_.Finish(reply, Status::OK, this);
                return;
            }

            delete this;
        }

    private:
        NodeService::AsyncService* service_;
        ServerCompletionQueue* cq_;
        ServerContext ctx_;
        Node* node_;
        ClientReadRequest req_;
        ServerAsyncResponseWriter<ClientReadReply> responder_;
        enum CallStatus { CREATE, PROCESS, FINISH };
        CallStatus status_;
    };
};

Node* Node::instance = nullptr;

class Client : public msg::NodeService::Service{

    msg::NodeService::AsyncService* service_;  // pointer to async service
    std::unique_ptr<ServerCompletionQueue> cq_;
    std::unique_ptr<Server> server_;
    std::thread async_server_thread_;

    int client_id;
    mutex main_process;
    mutex queue_reqs;
    condition_variable cv_main_process;
    condition_variable cv_queue_reqs;
    bool force_stop=false;
    queue<Client_Req> client_reqs;
    map<int,shared_ptr<msg::NodeService::Stub>> node_stubs;
    int leader;
    int nodes_num;
    bool retry;
    thread client_server;
    chrono::milliseconds client_timer;
    struct ResponseTracker {
        std::set<int> success_senders;
        std::set<int> failure_senders;
    };
    std::map<int, ResponseTracker> response_trackers;
    std::set<int> completed_requests;
    std::mt19937 rng;
    std::uniform_int_distribution<int> unique_dist;

public:
    Client(int client_id,int nodes_num) {
        this->service_ = new msg::NodeService::AsyncService();
        this->client_id=client_id;
        this->leader=1;
        this->nodes_num=nodes_num;
        this->client_timer=chrono::milliseconds(60*1000);
        std::random_device rd;
        this->rng.seed(rd() ^ (static_cast<unsigned>(client_id) << 16));
        this->unique_dist = std::uniform_int_distribution<int>(1, std::numeric_limits<int>::max());
        this->retry=false;

        for (int k = 1; k <= nodes_num; ++k) {
            string address = "localhost:" + to_string(5100 + k);
            auto channel = CreateChannel(address, InsecureChannelCredentials());
            auto stub = msg::NodeService::NewStub(channel);
            this->node_stubs.emplace(k, move(stub));
        }

        thread(&Client::run_async_server,this,this->client_id).detach();
        thread(&Client::run_server,this,this->client_id).detach();
        thread(&Client::send_node, this).detach();
        cout<<"Client : "<<this->client_id<<" created"<<endl;

    }

    ~Client() {
        if (server_) server_->Shutdown();
        if (cq_) cq_->Shutdown();
        if (async_server_thread_.joinable()) async_server_thread_.join();
        delete service_;
    }
    
    void run_server(int client_id) {
        ServerBuilder builder;
        string address = "localhost:" + to_string(6000 + client_id);
        builder.AddListeningPort(address, InsecureServerCredentials());
        builder.RegisterService(this);
        unique_ptr<Server> server(builder.BuildAndStart());
        // cout << "[" << client_id << "] Server listening on " << address << endl;
        server->Wait();
    }

    void run_async_server(int client_id) {
        // allocate async service if not already
        if (!service_) service_ = new msg::NodeService::AsyncService();

        ServerBuilder builder;
        string address = "localhost:" + to_string(6100 + client_id);
        builder.AddListeningPort(address, InsecureServerCredentials());

        // Register only the AsyncService for async handlers
        builder.RegisterService(service_);

        // Add completion queue for async
        cq_ = builder.AddCompletionQueue();

        // Save server to member (not a local)
        server_ = builder.BuildAndStart();
        HandleRpcs();
    }

    void HandleRpcs() {
        
        new AsyncTransCompletedCall(service_, cq_.get(),this);
        void* tag;  // uniquely identifies a request.
        bool ok;
        while (true) {
            cq_->Next(&tag, &ok);
            static_cast<AsyncTransCompletedCall*>(tag)->Proceed();
        }
        cout<<"Ending"<<endl;
    }

    void send_node(){
        unique_lock<mutex> lock(queue_reqs);
        while(true){
            if(!this->client_reqs.empty()){
                // Logic to send to node

                if (this->client_reqs.front().tns.first.second == "Read") {
                    const std::string read_key = this->client_reqs.front().tns.first.first;
                    this->client_reqs.pop();
                    lock.unlock();
                    auto read_results = this->collect_read_results(read_key);
                    this->print_read_results(read_key, read_results);
                    lock.lock();
                    continue;
                }

                ClientTransaction req;
                req.set_c1(this->client_id);
                Transaction* tns_ptr = req.mutable_tns();
                Clients* c = tns_ptr->mutable_c();
                c->set_c1(client_reqs.front().tns.first.first);
                c->set_c2(client_reqs.front().tns.first.second);
                tns_ptr->set_amt(client_reqs.front().tns.second);
                if(client_reqs.front().unique == -1){
                    client_reqs.front().unique = unique_dist(rng); // monotonic across client lifetime
                }
                int pending_unique = client_reqs.front().unique;
                req.set_unique(pending_unique);
                this->response_trackers.erase(pending_unique);
                this->completed_requests.erase(pending_unique);
                // context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

                if(!this->retry){
                    TransAck reply;
                    ClientContext context;
                    CompletionQueue cq;
                    Status status;
                    
                    std::unique_ptr<ClientAsyncResponseReader<TransAck> > rpc(this->node_stubs[this->leader]->AsyncSendClientTransaction(&context, req, &cq));

                    rpc->Finish(&reply, &status, (void*)nullptr);
                    
                    // this->leader=-1;
                    this->retry=true;
                }
                else{
                    this->leader=-1;
                    // cout<<"Client :"<<this->client_id<<" Sending Tns to All Nodes"<<endl;
                    for (int k=1;k<=this->nodes_num;k++){
                        TransAck reply;
                        ClientContext context;
                        CompletionQueue cq;
                        Status status;
                        
                        std::unique_ptr<ClientAsyncResponseReader<TransAck> > rpc(this->node_stubs[k]->AsyncSendClientTransaction(&context, req, &cq));
    
                        rpc->Finish(&reply, &status, (void*)nullptr);
                    }
                }
            }
            if(this->force_stop) break;
            cv_queue_reqs.wait_for(lock,chrono::milliseconds(this->client_timer));
            // cout<<"Client ID : "<<this->client_id<<" Queue Size : "<<this->client_reqs.size()<<endl;
        }
    }

    void keep_main_process_alive(){
        unique_lock<mutex> lock(main_process);
        while(cv_main_process.wait_for(lock,chrono::seconds(1),[this](){
            return !this->force_stop;
        }));
    }

    Status SendClientTransaction(ServerContext*c, const ClientTransaction*req, TransAck*reply) override{
        unique_lock<mutex> lock(queue_reqs);
        // this->client_reqs.push(Client_Req({{req->tns().c().c1(),req->tns().c().c2()},req->tns().amt()},req->c1(),req->unique()));
        this->client_reqs.push(Client_Req({{req->tns().c().c1(),req->tns().c().c2()},req->tns().amt()},req->c1(),-1));
        // cout<<"Client :"<<this->client_id<<" received Transaction"<<endl;
        lock.unlock();
        cv_queue_reqs.notify_all();
        return Status::OK;
    }
    
    Status SendClientReset(ServerContext*c, const ClientTransaction*req, TransAck*reply) override{
        unique_lock<mutex> lock(queue_reqs);
        if(req->msg()=="ResetClient"){
            while(!this->client_reqs.empty()) this->client_reqs.pop();
            this->leader=1;
            this->response_trackers.clear();
            this->completed_requests.clear();
            this->retry=false;
        }
        return Status::OK;
    }

private:
    class AsyncTransCompletedCall;
    friend class AsyncTransCompletedCall;

    int required_matching_responses() const {
        int f = std::max(1, (this->nodes_num - 1) / 3);
        return std::max(1, 2 * f + 1);
    }

    bool remove_request_by_unique_locked(int unique) {
        std::queue<Client_Req> remaining;
        bool removed = false;
        while (!this->client_reqs.empty()) {
            Client_Req current = this->client_reqs.front();
            this->client_reqs.pop();
            if (!removed && current.unique == unique) {
                removed = true;
                continue;
            }
            remaining.push(current);
        }
        this->client_reqs = std::move(remaining);
        return removed;
    }

    void finalize_request_locked(int unique, bool success) {
        if (!this->completed_requests.insert(unique).second) {
            return;
        }

        this->response_trackers.erase(unique);

        bool removed = this->remove_request_by_unique_locked(unique);
        if (removed) {
            if (success) {
                std::cout << "Client " << this->client_id
                          << " confirmed successful execution of transaction Unique_id="
                          << unique << std::endl;
            } else {
                std::cout << "Client " << this->client_id
                          << " confirmed failed execution of transaction Unique_id="
                          << unique << std::endl;
            }
        }
    }

    ReadConsensusResult collect_read_results(const std::string& key) {
        ClientReadRequest req;
        req.set_requester_id(this->client_id);
        req.set_key(key);

        grpc::CompletionQueue cq;

        struct AsyncReadState {
            int node_id = 0;
            std::unique_ptr<ClientContext> context;
            std::unique_ptr<ClientAsyncResponseReader<ClientReadReply>> rpc;
            ClientReadReply reply;
            Status status;
        };

        std::vector<std::unique_ptr<AsyncReadState>> states;
        states.reserve(this->node_stubs.size());

        for (auto& [node_id, stub] : this->node_stubs) {
            auto state = std::make_unique<AsyncReadState>();
            state->node_id = node_id;
            state->context = std::make_unique<ClientContext>();
            state->rpc = stub->AsyncSendClientRead(state->context.get(), req, &cq);
            state->rpc->Finish(&state->reply, &state->status, state.get());
            states.push_back(std::move(state));
        }

        ReadConsensusResult result;
        result.supporting_entries.reserve(states.size());

        std::map<std::pair<bool, int>, std::set<int>> consensus_tracker;
        std::map<int, ClientReadReplyEntry> latest_by_sender;
        std::optional<std::pair<bool, int>> consensus_key;
        const int threshold = this->required_matching_responses();

        size_t completed = 0;
        void* tag = nullptr;
        bool ok = false;

        while (completed < states.size() && cq.Next(&tag, &ok)) {
            auto* state = static_cast<AsyncReadState*>(tag);
            if (!ok || !state->status.ok()) {
                ClientReadReplyEntry entry;
                entry.set_sender_id(state->node_id);
                entry.set_key(key);
                entry.set_amount(0);
                entry.set_found(false);
                latest_by_sender[state->node_id] = entry;
            } else {
                for (const auto& entry : state->reply.entries()) {
                    latest_by_sender[entry.sender_id()] = entry;
                    std::pair<bool, int> key_pair(entry.found(), entry.found() ? entry.amount() : 0);
                    auto& senders = consensus_tracker[key_pair];
                    senders.insert(entry.sender_id());
                    if (!consensus_key.has_value() && static_cast<int>(senders.size()) >= threshold) {
                        consensus_key = key_pair;
                    }
                }
            }
            completed++;
            if (consensus_key.has_value()) {
                break;
            }
        }

        if (consensus_key.has_value()) {
            result.consensus_reached = true;
            result.value_found = consensus_key->first;
            result.amount = consensus_key->second;
            const auto& senders = consensus_tracker[*consensus_key];
            for (int sender_id : senders) {
                auto it = latest_by_sender.find(sender_id);
                if (it != latest_by_sender.end()) {
                    result.supporting_entries.push_back(it->second);
                } else {
                    ClientReadReplyEntry synthetic;
                    synthetic.set_sender_id(sender_id);
                    synthetic.set_key(key);
                    synthetic.set_found(result.value_found);
                    synthetic.set_amount(result.value_found ? result.amount : 0);
                    result.supporting_entries.push_back(synthetic);
                }
            }
            for (auto& state : states) {
                if (state->context) {
                    state->context->TryCancel();
                }
            }
        } else {
            for (const auto& [sender_id, entry] : latest_by_sender) {
                result.supporting_entries.push_back(entry);
            }
        }

        cq.Shutdown();
        while (cq.Next(&tag, &ok)) {
            // drain any remaining events after shutdown
        }
        return result;
    }

    void print_read_results(const std::string& key, const ReadConsensusResult& result) {
        if (result.supporting_entries.empty()) {
            std::cout << "Client " << this->client_id
                      << " did not receive read responses for key " << key << std::endl;
            return;
        }

        if (result.consensus_reached) {
            if (result.value_found) {
                std::cout << "Client " << this->client_id << " read consensus for key "
                          << key << " = " << result.amount
                          << " (" << result.supporting_entries.size() << " nodes)" << std::endl;
            } else {
                std::cout << "Client " << this->client_id
                          << " reached consensus that key " << key
                          << " is missing (" << result.supporting_entries.size()
                          << " nodes)" << std::endl;
            }
        } else {
            std::cout << "Client " << this->client_id
                      << " did not reach consensus for key " << key
                      << "; partial responses:" << std::endl;
            for (const auto& entry : result.supporting_entries) {
                if (entry.found()) {
                    std::cout << "  Node " << entry.sender_id() << " → "
                              << entry.key() << " = " << entry.amount() << std::endl;
                } else {
                    std::cout << "  Node " << entry.sender_id() << " → "
                              << entry.key() << " not found" << std::endl;
                }
            }
        }
    }

    class AsyncTransCompletedCall {
    public:
        msg::NodeService::AsyncService* service;
        grpc::ServerCompletionQueue* cq;
        grpc::ServerContext ctx;
        TransCompleted request;
        Ack reply;
        grpc::ServerAsyncResponseWriter<Ack> responder;
        Client* client;

        enum CallStatus { CREATE, PROCESS, FINISH };
        CallStatus status;

        // Constructor ARMS this object for the next incoming RPC.
        AsyncTransCompletedCall(msg::NodeService::AsyncService* svc,
                                grpc::ServerCompletionQueue* queue,
                                Client* client)
            : service(svc), cq(queue), responder(&ctx), client(client), status(CREATE) {
            Proceed();
            // cout<<11<<endl;
            
        }
        
        void Proceed() {
            if (status == CREATE) {
                // cout<<12<<endl;
                
                service->RequestSendTransCompleted(&ctx, &request, &responder, cq, cq, this);
                status = PROCESS;
            }
            
            else if (status == PROCESS) {
                // cout<<13<<endl;
                
                // Process the request quickly while holding minimal locks
                {
                    std::lock_guard<std::mutex> lk(client->queue_reqs);
                    // client->leader = request.sender_id();
                    client->leader=((request.b().ballot_num()-1) % client->nodes_num) + 1;
                    const int unique_id = request.unique();
                    if (unique_id != -1 && !client->completed_requests.count(unique_id)) {
                        auto& tracker = client->response_trackers[unique_id];
                        if (request.r()) {
                            tracker.failure_senders.erase(request.sender_id());
                        } else {
                            tracker.success_senders.erase(request.sender_id());
                        }

                        std::set<int>& bucket = request.r()
                            ? tracker.success_senders
                            : tracker.failure_senders;
                        bucket.insert(request.sender_id());

                        const int matches = static_cast<int>(bucket.size());
                        const int threshold = client->required_matching_responses();
                        if (matches == threshold) {
                            client->finalize_request_locked(unique_id, request.r());
                            client->retry=false;
                            client->cv_queue_reqs.notify_all();
                        }
                    }
                }
                new AsyncTransCompletedCall(service, cq, client);
                
                status = FINISH;
                responder.Finish(reply, Status::OK, this);
                // Reply and arrange for final cleanup when Finish completes
            }
            else{
                // FINISH: Final callback after Finish() completes — delete the object.
                // cout<<14<<endl;
                delete this;
            }
        }
    };

};


vector<pid_t> pids;
vector<pid_t> client_pids;
map<int, vector<BatchOperation>> set_transactions;
map<int, vector<bool>> set_live_nodes;
map<int, vector<int>> set_byzantine_nodes;
map<int, vector<std::string>> set_attack_strategy;

void handle_sigint(int) {
    std::cout << "\nSIGINT received. Killing all child processes..." << std::endl;

    for (pid_t pid : pids) {
        if (pid > 0) {
            kill(pid, SIGTERM);  // send termination signal
            waitpid(pid, nullptr, 0);  // reap the process
            std::cout << "Killed process PID " << pid << std::endl;
        }
    }
    for (pid_t pid : client_pids) {
        if (pid > 0) {
            kill(pid, SIGTERM);  // send termination signal
            waitpid(pid, nullptr, 0);  // reap the process
            std::cout << "Killed process PID " << pid << std::endl;
        }
    }

    std::cout << "All child processes terminated. Exiting parent." << std::endl;
    exit(0);  // exit parent after cleanup
}

void handle_sigsegv(int sig) {
    std::cerr << "Segmentation fault detected! Cleaning up...\n";

    // Kill all child processes
    for (pid_t pid : pids) {
        if (kill(pid, SIGKILL) == 0) {
            std::cerr << "Killed process " << pid << std::endl;
        }
    }

    // Optionally wait for children to terminate
    for (pid_t pid : pids) {
        waitpid(pid, nullptr, 0);
    }

    std::_Exit(1); // exit immediately
}

void read_batches_from_csv(const std::string& filename) {
    std::ifstream file(filename);
    if (!file.is_open()) {
        cerr << "Could not open file: " << filename << endl;
        return;
    }

    std::string line;
    if (!std::getline(file, line)) {
        return; // empty file
    }

    int prev_set = -1;
    const std::size_t kMaxNodeSlots = 10; // allow datasets to reference up to 10 nodes

    auto trim_edges = [](std::string& s) {
        auto not_space = [](unsigned char ch) { return !std::isspace(ch); };
        s.erase(s.begin(), std::find_if(s.begin(), s.end(), not_space));
        s.erase(std::find_if(s.rbegin(), s.rend(), not_space).base(), s.end());
    };

    while (std::getline(file, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }

        std::vector<std::string> cols;
        std::string cur;
        bool in_quotes = false;
        for (char ch : line) {
            if (ch == '"') {
                in_quotes = !in_quotes;
            } else if (ch == ',' && !in_quotes) {
                cols.push_back(cur);
                cur.clear();
            } else {
                cur.push_back(ch);
            }
        }
        cols.push_back(cur);
        while (cols.size() < 5) {
            cols.push_back("");
        }

        std::string set_str = cols[0];
        std::string txn_str = cols[1];
        std::string nodes_str = cols[2];
        std::string byzantine_str = cols[3];
        std::string attack_str = cols[4];

        trim_edges(set_str);

        int set_number = prev_set;
        if (!set_str.empty()) {
            try {
                set_number = std::stoi(set_str);
                prev_set = set_number;
            } catch (const std::exception& ex) {
                std::cerr << "Skipping row with invalid set number '" << set_str
                          << "': " << ex.what() << std::endl;
                continue;
            }
        }

        if (set_number == -1) {
            continue;
        }

        auto strip_quotes = [](std::string& s) {
            s.erase(std::remove(s.begin(), s.end(), '"'), s.end());
        };

        // Normalize attack descriptor (column 5).
        strip_quotes(attack_str);
        trim_edges(attack_str);
        if (attack_str == "[]") {
            set_attack_strategy[set_number] = {};
        } else if (!attack_str.empty()) {
            if (attack_str.size() >= 2 && attack_str.front() == '[' && attack_str.back() == ']') {
                attack_str = attack_str.substr(1, attack_str.size() - 2);
            }

            std::vector<std::string> attacks;
            std::stringstream attack_ss(attack_str);
            std::string attack_token;
            while (std::getline(attack_ss, attack_token, ';')) {
                trim_edges(attack_token);
                if (!attack_token.empty() && attack_token.front() == ',') {
                    attack_token.erase(attack_token.begin());
                    trim_edges(attack_token);
                }
                if (!attack_token.empty() && attack_token.back() == ',') {
                    attack_token.pop_back();
                    trim_edges(attack_token);
                }
                if (!attack_token.empty()) {
                    attacks.push_back(attack_token);
                }
            }

            set_attack_strategy[set_number] = attacks;
        }

        // Normalize byzantine node list (column 4).
        strip_quotes(byzantine_str);
        trim_edges(byzantine_str);
        if (byzantine_str == "[]") {
            set_byzantine_nodes[set_number] = {};
        } else if (!byzantine_str.empty()) {
            if (byzantine_str.size() >= 2 && byzantine_str.front() == '[' && byzantine_str.back() == ']') {
                byzantine_str = byzantine_str.substr(1, byzantine_str.size() - 2);
            }

            std::stringstream byzantine_ss(byzantine_str);
            std::string node;
            std::vector<int> nodes;
            while (std::getline(byzantine_ss, node, ',')) {
                trim_edges(node);
                if (node.empty()) {
                    continue;
                }
                node.erase(std::remove(node.begin(), node.end(), 'n'), node.end());
                trim_edges(node);
                if (node.empty()) {
                    continue;
                }
                try {
                    int node_id = std::stoi(node);
                    if (node_id > 0) {
                        nodes.push_back(node_id);
                    }
                } catch (const std::exception& ex) {
                    std::cerr << "Skipping invalid byzantine node entry '" << node << "' for set "
                              << set_number << ": " << ex.what() << std::endl;
                }
            }

            if (!nodes.empty()) {
                set_byzantine_nodes[set_number] = nodes;
            }
        }

        // Normalize transaction payload (column 2).
        strip_quotes(txn_str);
        trim_edges(txn_str);
        if (!txn_str.empty()) {
            if (txn_str.find("LF") != std::string::npos) {
                set_transactions[set_number].push_back({BatchOpType::LF, "LF", "LF", 0});
            } else {
                txn_str.erase(std::remove(txn_str.begin(), txn_str.end(), '('), txn_str.end());
                txn_str.erase(std::remove(txn_str.begin(), txn_str.end(), ')'), txn_str.end());

                std::stringstream txn_ss(txn_str);
                std::string from, to, amt_str;
                std::getline(txn_ss, from, ',');
                std::getline(txn_ss, to, ',');
                std::getline(txn_ss, amt_str, ',');

                trim_edges(from);
                trim_edges(to);
                trim_edges(amt_str);

                if (!from.empty() && to.empty() && amt_str.empty()) {
                    set_transactions[set_number].push_back({BatchOpType::Read, from, "", 0});
                } else if (!from.empty() && !to.empty() && !amt_str.empty()) {
                    try {
                        int amt = std::stoi(amt_str);
                        set_transactions[set_number].push_back({BatchOpType::Transfer, from, to, amt});
                    } catch (const std::exception& ex) {
                        std::cerr << "Skipping transaction with invalid amount '" << amt_str
                                  << "' for set " << set_number << ": " << ex.what() << std::endl;
                    }
                } else {
                    std::cerr << "Skipping unrecognized transaction entry '" << txn_str
                              << "' for set " << set_number << std::endl;
                }
            }
        }

        // Normalize live node vector (column 3).
        strip_quotes(nodes_str);
        trim_edges(nodes_str);
        if (nodes_str == "[]") {
            set_live_nodes[set_number] = std::vector<bool>(kMaxNodeSlots, false);
        } else if (!nodes_str.empty()) {
            nodes_str.erase(std::remove(nodes_str.begin(), nodes_str.end(), '['), nodes_str.end());
            nodes_str.erase(std::remove(nodes_str.begin(), nodes_str.end(), ']'), nodes_str.end());

            std::stringstream nodes_ss(nodes_str);
            std::string node;
            std::vector<bool> nodes;
            while (std::getline(nodes_ss, node, ',')) {
                trim_edges(node);
                if (node.empty()) {
                    continue;
                }
                node.erase(std::remove(node.begin(), node.end(), 'n'), node.end());
                trim_edges(node);
                if (node.empty()) {
                    continue;
                }
                try {
                    int idx = std::stoi(node) - 1;
                    if (idx < 0) {
                        continue;
                    }
                    if (static_cast<std::size_t>(idx) >= nodes.size()) {
                        nodes.resize(idx + 1, false);
                    }
                    nodes[idx] = true;
                } catch (const std::exception& ex) {
                    std::cerr << "Skipping invalid node entry '" << node << "' for set "
                              << set_number << ": " << ex.what() << std::endl;
                }
            }

            if (nodes.size() < kMaxNodeSlots) {
                nodes.resize(kMaxNodeSlots, false);
            }
            set_live_nodes[set_number] = nodes;
        }
    }
}


int main(){
    signal(SIGINT, handle_sigint);


    std::ofstream outfile("view/logs_new_view.txt", std::ios::out | std::ios::trunc);
    outfile<<"";
    outfile.close();

    map<int,shared_ptr<msg::NodeService::Stub>> node_stubs;
    map<int,shared_ptr<msg::NodeService::Stub>> client_stubs;
    read_batches_from_csv("test.csv");
    map<string,int> db;
    int n_nodes = 7;
    int n_clients=10;

    int set_num=1;

    vector<bool> cur_live(n_nodes,true);
    for(char x='A';x<='J';x++){
        db.emplace(string(1,x),10);
        // cout<<string(1,x)<<" "<<10<<endl;
    }

    CryptoPP::AutoSeededRandomPool rng;
    vector<CryptoPP::RSA::PrivateKey> node_private_keys(n_nodes);
    map<int, CryptoPP::RSA::PublicKey> node_public_keys;
    for (int idx = 0; idx < n_nodes; ++idx) {
        CryptoPP::InvertibleRSAFunction params;
        params.GenerateRandomWithKeySize(rng, 2048);
        CryptoPP::RSA::PrivateKey priv(params);
        CryptoPP::RSA::PublicKey pub; 
        pub.Initialize(params.GetModulus(), params.GetPublicExponent());

        node_private_keys[idx] = priv;
        node_public_keys[idx + 1] = pub;
    }

    for (int node_id = 1; node_id <= n_nodes; ++node_id) {
        pid_t pid = fork();
        if(pid==0){
            // Node node(stubs, node_id,n);  
            Node node(node_id,n_nodes,db,n_clients,node_private_keys[node_id-1],node_public_keys);
            node.keep_main_process_alive();
        }
        else{
            pids.push_back(pid);
        }
    }
    for (int client_id = 1; client_id <= n_clients; ++client_id) {
        pid_t pid = fork();
        if(pid==0){
            // Node node(stubs, client_id,n);  
            Client client(client_id,n_nodes);
            client.keep_main_process_alive();
        }
        else{
            pids.push_back(pid);
        }
    }
    
    for (int node_id = 1; node_id <= n_nodes; ++node_id) {
        string address = "localhost:" + to_string(5000 + node_id);
        auto channel = CreateChannel(address, InsecureChannelCredentials());
        auto stub = msg::NodeService::NewStub(channel);
        node_stubs.emplace(node_id, move(stub));
    }
    for (int client_id = 1; client_id <= n_clients; ++client_id) {
        string address = "localhost:" + to_string(6000 + client_id);
        auto channel = CreateChannel(address, InsecureChannelCredentials());
        auto stub = msg::NodeService::NewStub(channel);
        client_stubs.emplace(client_id, move(stub));
    }

    // while(true){
    //     pause();
    // }
        
    sleep(5);
    while (true) {
        std::cout << "\n===== MENU =====\n";
        std::cout << "1. Execute transaction\n";
        std::cout << "2. Print logs\n";
        std::cout << "3. Print DB\n";
        std::cout << "4. Print Status\n";
        std::cout << "6. New Batch\n";
        std::cout << "7. Exit Press Ctrl +C\n";
        std::cout << "Choose: ";

        int choice;
        std::cin >> choice;


        // std::string folder = "logs";
        mkdir("logs", 0777);
        mkdir("db", 0777);
        mkdir("view", 0777);

        // mkdir("status", 0777);

        if(choice==1){
            Command req;
            req.set_type("TriggerTns");
            for(int i=1;i<=n_nodes;i++){
                Ack reply;
                ClientContext context;
                Status status = node_stubs[i]->ActionCommand(&context, req, &reply);
            }
        }

        else if(choice==2){
            Command req;
            req.set_type("PrintLog");
            for(int i=1;i<=n_nodes;i++){
                Ack reply;
                ClientContext context;
                Status status = node_stubs[i]->ActionCommand(&context, req, &reply);
            }
        }

        else if(choice==3){
            Command req;
            req.set_type("PrintDB");
            for(int i=1;i<=n_nodes;i++){
                Ack reply;
                ClientContext context;
                Status status = node_stubs[i]->ActionCommand(&context, req, &reply);
            }
        }

        else if(choice==4){
            Command req;
            int temp;
            cout<<"Enter a Sequence Number :";
            cin>>temp;
            req.set_type("PrintStatus");
            req.set_seq_num(temp);
            for(int i=1;i<=n_nodes;i++){
                Ack reply;
                ClientContext context;
                Status status = node_stubs[i]->ActionCommand(&context, req, &reply);
            }
        }

        else if (choice == 6) {

            ClientTransaction req_c;
            req_c.set_msg("ResetClient");
            for(int i=1;i<=n_clients;i++){
                TransAck reply;
                ClientContext context;
                Status status = client_stubs[i]->SendClientReset(&context, req_c, &reply);
            }
            Command req;
            req.set_type("ResetNode");
            for(int i=1;i<=n_nodes;i++){
                Ack reply;
                ClientContext context;
                Status status = node_stubs[i]->ActionCommand(&context, req, &reply);
            }

            for(string x: set_attack_strategy[set_num]){
                Command req; 
                req.set_type(x);
                // if(x=="crash"){
                for(int c_n: set_byzantine_nodes[set_num]){
                    Ack reply;
                    ClientContext context;
                    Status status = node_stubs[c_n]->ActionCommand(&context, req, &reply);
                }
                // }
                // else if(x.rfind("equivocation", 0) == 0||x=="time"){

                // }
            }

            cout<<"Loading Set :"<<set_num<<endl;
            if(set_live_nodes.find(set_num)==set_live_nodes.end()){
                std::cout << "No more batches available.\n";
                continue;
            }

            for(int i=0;i<n_nodes;i++){
                if(cur_live[i]==set_live_nodes[set_num][i])continue;
                else if(cur_live[i]){
                    cur_live[i]=false;
                    Command req; 
                    req.set_type("StopProcess"); 
                    cout<<"Stopping Node :"<<i+1<<endl; 
                    Ack reply; 
                    ClientContext context; 
                    Status status = node_stubs[i+1]->ActionCommand(&context, req, &reply);
                }
                else{
                    cur_live[i]=true;
                    Command req; 
                    req.set_type("ResumeProcess"); 
                    cout<<"Resuming Node :"<<i+1<<endl; 
                    Ack reply; 
                    ClientContext context; 
                    Status status = node_stubs[i+1]->ActionCommand(&context, req, &reply);
                }
            }
            // cout<<"Completed With Current Set of Nodes"<<endl;
            for (const auto& op : set_transactions[set_num]){

                ClientTransaction req;
                TransAck reply;
                ClientContext context;

                req.set_c1(op.from.empty() ? 0 : op.from[0]);
                Transaction* tns_ptr = req.mutable_tns();
                Clients* c = tns_ptr->mutable_c();
                c->set_c1(op.from);
                if (op.type == BatchOpType::Read) {
                    c->set_c2("Read");
                    tns_ptr->set_amt(0);
                } else {
                    c->set_c2(op.to);
                    tns_ptr->set_amt(op.amount);
                }
                int client_index = op.from.empty() ? -1 : (op.from[0] - 64);
                if (client_index < 1 || client_index > n_clients) {
                    std::cerr << "Skipping request for client '" << op.from
                              << "' due to invalid client index" << std::endl;
                    continue;
                }
                Status status = client_stubs[client_index]->SendClientTransaction(&context, req, &reply);
                // cout<<"Main Sent to "<<tns.first.first[0]-64<<endl;
            }
            set_num++;
        }
        else if (choice == 7) {
            break;
        }
        sleep(2);
    }

    std::cout << "All workers stopped. Cleanup complete." << std::endl;
}