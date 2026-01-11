// #include <bits/stdc++.h>
#include <iostream>
#include <string>
#include <map>
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
#include <sys/types.h>
#include <sys/stat.h>
#include <filesystem>

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
        bool is_leader;
        bool timer_stopped;
        bool reset_flag;
        bool election_completed;
        bool trigger_tns;
        int ballot_num;
        int cur_leader;
        int seq_num;
        int votes;
        int nodes_num;
        int execution_index;
        bool running_election;
        bool stop_process;
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
        mutex seq_num_mutex;
        mutex commit_set_mutex;
        mutex processing_msg;
        mutex temp_mutex;
        mutex vote_mutex;
        mutex form_new_log_view_mutex;
        condition_variable cv;
        condition_variable cv_election;
        condition_variable election_cv;
        condition_variable cv_stop_process;
        condition_variable cv_main_process;
        condition_variable cv_log_entry_or_heatbeat;
        condition_variable cv_vote;
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
        static Node* instance;
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

    // Node (map<int,shared_ptr<msg::NodeService::Stub>> stubs,int node_id,int nodes_num){
    Node (int node_id,int nodes_num,map<string,int>db, int client_num){

        instance = this;
        signal(SIGINT, Node::signalHandler);
        // signal(SIGTERM, Node::signalHandler);

        // signal(SIGSEGV, Node::signalHandler);
        // signal(SIGABRT, Node::signalHandler);

        std::atexit([](){
            if (Node::instance) {
                pid_t pid = getpid();
                if (Node::instance->is_leader)
                    std::cout << "[Leader Node " << Node::instance->node_id
                              << " | pid " << pid << "] exiting" << std::endl;
                else
                    std::cout << "[Node " << Node::instance->node_id
                              << " | pid " << pid << "] exiting" << std::endl;
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
        // this->stubs=move(stubs);
        this->node_id=node_id;
        this->nodes_num=nodes_num;
        this->ballot_num=0;
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

        auto now = std::chrono::high_resolution_clock::now().time_since_epoch().count();
        unsigned seed = static_cast<unsigned>(now ^ (getpid() * 0x9e3779b97f4a7c15ULL));
        std::mt19937 rng(seed);  // Mersenne Twister seeded with time + PID
        std::uniform_int_distribution<int> dist(2000, 4000); // 150–300 ms timeout range
        this->random_timeout = dist(rng);
        this->timeout = std::chrono::milliseconds(random_timeout);
        this->heartbeat_timer = std::chrono::milliseconds(500);

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
            // this->timer_stopped=true;
            thread(&Node::start_election, this).detach(); //Testing
        }
        // else{
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
        // cout << "[" << node_id << "] Server listening on " << address << endl;
        server->Wait();
    }

    void run_async_server(int node_id) {
        // allocate async service if not already
        if (!service_) service_ = new msg::NodeService::AsyncService();

        ServerBuilder builder;
        string address = "localhost:" + to_string(5100 + node_id);
        builder.AddListeningPort(address, InsecureServerCredentials());

        // Register only the AsyncService for async handlers
        builder.RegisterService(service_);

        // Add completion queue for async
        cq_ = builder.AddCompletionQueue();

        // Save server to member (not a local)
        server_ = builder.BuildAndStart();
        // cout << "[" << node_id << "] Async Server listening on " << address << endl;

        // Create the initial handler that arms the RPC
        // new AsyncClientTransactionCall(service_, cq_.get(), this);

        // Start the CQ processing thread (do not detach)
        // async_server_thread_ = std::thread([this]() { this->HandleRpcs(); });

        async_server_thread_ = std::thread(&Node::HandleRpcs, this);
        // HandleRpcs();
        // Option: block here or return — if you want run_server to block, you can call server_->Wait()
        server_->Wait(); // if you want this thread to block here
    }

    // class CallData {
    // public:
    //     virtual void Proceed();
    //     virtual ~CallData() {}
    // };

    // void HandleRpcs() {
    //     void* tag;
    //     bool ok;
    //     while (cq_->Next(&tag, &ok)) {
    //         cout<<"Imp"<<endl;
    //         CallData* cd = static_cast<CallData*>(tag);
    //         cd->Proceed(ok);
    //     }
    // }

    void HandleRpcs() {
    // Spawn a new CallData instance to serve new clients.
        new AsyncSendClientTransactionCall(service_, cq_.get(),this);
        void* tag;  // uniquely identifies a request.
        bool ok;
        while (true) {
        // Block waiting to read the next event from the completion queue. The
        // event is uniquely identified by its tag, which in this case is the
        // memory address of a CallData instance.
        // The return value of Next should always be checked. This return value
        // tells us whether there is any kind of event or cq_ is shutting down.
        //   CHECK(cq_->Next(&tag, &ok));
        //   CHECK(ok);
            // cout<<"Waiting for event"<<endl;
            // cq_->Next(&tag, &ok);
            if (!cq_->Next(&tag, &ok)) break;  
            if (!ok) {                                     // event failed/cancelled
                // If tag is a valid CallData pointer you control, cast and delete safely.
                // Otherwise, just continue/break depending on your design.
                continue;
            }
            static_cast<AsyncSendClientTransactionCall*>(tag)->Proceed(); 
            // cout<<"Got new event"<<endl;
            // static_cast<AsyncSendClientTransactionCall*>(tag)->Proceed();
        }
        // cout<<"Ending"<<endl;
    }


    void start_timer() {
        unique_lock<mutex> lock(time_mutex);

        if(this->node_id!=1)this->timer_stopped=false;
        else this->timer_stopped=true;

        while (true) {
            bool timed_out = !cv.wait_for(lock, timeout, [this]() { return this->reset_flag; });

            unique_lock<mutex> lock_vote(vote_mutex);
            cv_vote.wait_for(lock_vote, std::chrono::milliseconds(50), [this]() { return this->reset_flag; });

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
                // Timeout expired → trigger election
                this->running_election = true;
                std::cout << "Node " << this->node_id << " timer expired after " << timeout.count() << "ms, starting election\n";
                std::thread(&Node::start_election, this).detach();
                stopped_mutex.lock();
                this->timer_stopped=true;
                stopped_mutex.unlock();
                // break;
            }
            
            // stopped_mutex.lock();
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

    void start_election() { //add if election already running then return
        // if(this->running_election) return;
        this->form_new_log_view_mutex.lock();
        this->running_election=true;
        this->stop_election=false;
        this->election_completed=false;
        this->trigger_tns=false;
        ballot_num++;

        cout<<"Node :"<<node_id<<" starting election with ballot number :"<<ballot_num<<"\n";

        this->logs_mutex.lock(); //  Acquiring before election to parallely make new log
        this->votes=1; // Vote for itself
        
        for (auto& [pid, stub] : this->stubs) {
            if(pid==this->node_id) continue;
            // election_threads.emplace_back(&Node::send_prepare, this, pid);
            thread(&Node::send_prepare, this, pid).detach();
        }

        vector<Log> new_view_logs;
        int cur_seq=1;
        // int ctr=0;
        int temp_ballot=1;
        int temp_sender_id=this->node_id;
        for (const Log& cur_log : this->logs) {

            if(this->execution_index+1>cur_seq||commit_set.find(cur_seq)!=commit_set.end()){
                new_view_logs.push_back(cur_log);
            }
            else{
                // if(cur_seq>=this->seq_num) break;

                // while(cur_seq<cur_log.seq_num){
                //     new_view_logs.push_back(Log(pair<int,int>({temp_ballot,temp_sender_id}),cur_seq,pair<pair<string,string>,int>({pair<string,string>({"noop","noop"}),0}),-1));
                //     cur_seq++;
                // }

                if(cur_log.unique!=-1) new_view_logs.push_back(Log(pair<int,int>({this->ballot_num,this->node_id}),cur_seq,pair<pair<string,string>,int>({pair<string,string>({cur_log.tns.first.first,cur_log.tns.first.second}),cur_log.tns.second}),cur_log.unique));
                else new_view_logs.push_back(Log(pair<int,int>({temp_ballot,temp_sender_id}),cur_seq,pair<pair<string,string>,int>({pair<string,string>({"noop","noop"}),0}),-1));
            }

            cur_seq++;
            // ctr++;

            temp_ballot=cur_log.b.first;
            temp_sender_id=cur_log.b.second;
        }
        // this->seq_num=max(this->seq_num,cur_seq); //Won't be needed as it is only using its own log
        // for(;ctr<this->logs.size();ctr++){
        //     new_view_logs.push_back(this->logs[ctr]);
        // }

        this->logs.swap(new_view_logs); // Check if there is need to make a copy or not

        this->logs_mutex.unlock();

        // cout<<"Node :"<<node_id<<" Waiting for election mutex\n"<<endl;

        // bool success=false; 

        // Start election timer
        while (true){
            // unique_lock<mutex> lock(election);
            unique_lock<mutex> lock(this->temp_mutex);
            cv_election.wait_for(lock, chrono::milliseconds(500));
            // lock.unlock();
            if(this->election_completed||this->is_leader){
                // cout<<"Node :"<<node_id<<" election completed\n"<<endl;
                // success=true;
                for (Log& cur_log : this->logs) {
                    if(cur_log.seq_num<=this->execution_index)continue;
                    cur_log.b.first=this->ballot_num;
                    cur_log.b.second=this->node_id;
                }
            }
            else{
                this->stop_election = true;
                cout<<"Node :"<<node_id<<" election stopped\n"<<endl;
            }
            break;
        }

        this->form_new_log_view_mutex.unlock();
        // for (auto& t : election_threads) {
        //     if (t.joinable()) t.join();
        // }

        // cout<<"Node :"<<node_id<<" New View Log Formed\n"<<endl;
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
            auto accepted_seq_count=make_shared<map<int,int>>();  //Dyamic allocation
            // NewLogView req;
            auto req = make_shared<NewLogView>();
            Ballot* b = req->mutable_b(); // returns pointer to nested message
            b->set_ballot_num(this->ballot_num);
            b->set_sender_id(this->node_id);

            new_view_mutex.lock();
            for (const Log& log : this->logs) {
                LogMsg* temp=req->add_log();
                Ballot* temp_b = temp->mutable_b();
                temp_b->set_ballot_num(log.b.first);
                temp_b->set_sender_id(log.b.second);
                temp->set_seq_num(log.seq_num);
                Transaction* temp_tns = temp->mutable_tns();
                temp_tns->set_unique(log.unique);
                Clients* temp_c = temp_tns->mutable_c();
                temp_c->set_c1(log.tns.first.first);
                temp_c->set_c2(log.tns.first.second);
                temp_tns->set_amt(log.tns.second);
                temp_tns->set_unique(log.unique);
                (*accepted_seq_count)[log.seq_num]=1;
            }
            new_view_mutex.unlock();

            // cout<<"Node :"<<node_id<<" Sending New View Log :"<<ballot_num<<"\n"<<endl;
            
            auto accepted_seq_count_mutex=make_shared<std::mutex>();  //Dyamic allocation
            for (auto& [pid, stub] : this->stubs) { //for new view
                if(pid==this->node_id) continue;
                thread(&Node::send_newviewlog, this, pid,req,accepted_seq_count,accepted_seq_count_mutex).detach();
            }
            // cout<<"Leader Sent New Log View"<<endl;
            thread(&Node::dump_new_view, this).detach();
            // sleep(2);
            // In start_election(), after becoming leader and sending NewLogView...
            
            while(!this->stop_process&&this->is_leader){
                if(this->trigger_tns&&!this->client_reqs.empty()){
                    // cout<<"ABABA"<<endl;

                    // logs_mutex.lock();
                    this->logs.push_back(Log({this->ballot_num,this->node_id},this->seq_num,this->client_reqs.front().tns,this->client_reqs.front().unique));
                    // logs_mutex.unlock();

                    this->client_reqs.pop();
                    
                    Accept* req=new Accept();
                    Ballot* b = req->mutable_b(); // returns pointer to nested message
                    b->set_ballot_num(this->ballot_num);
                    b->set_sender_id(this->node_id);
                    Transaction* tns = req->mutable_tns();
                    Clients* c = tns->mutable_c();
                    c->set_c1(this->logs[this->seq_num-1].tns.first.first);
                    c->set_c2(this->logs[this->seq_num-1].tns.first.second);
                    tns->set_amt(this->logs[this->seq_num-1].tns.second);
                    tns->set_unique(this->logs[this->seq_num-1].unique);
                    req->set_seq_num(this->seq_num);


                    // seq_num_mutex.lock();
                    this->seq_num++;
                    // seq_num_mutex.unlock();
                    
                    auto accepted_seq_count_mutex=make_shared<std::mutex>();  //Dyamic allocation
                    auto accepted_seq_val_count=make_shared<int>(1);  //Dyamic allocation
                    // auto accepted_seq_val_count_mutex=new mutex();
                    // int* accepted_seq_val_count= new int(1);
                    // this->accepted_seq_val_count=0;
                    cout<<"Node :"<<node_id<<" Sending Log Entry:"<<this->seq_num-1<<endl;
                    for (auto& [pid, stub] : this->stubs) {
                        if(pid==this->node_id) continue;
                        thread(&Node::send_accept, this, pid, new Accept(*req),accepted_seq_count_mutex,accepted_seq_val_count).detach();
                    }
                    cout<<"Node :"<<node_id<<" Sent Accept"<<endl;
                    if(this->client_reqs.empty())this->trigger_tns=false;
                }
                else{
                    this->trigger_tns=false;
                    unique_lock<mutex> logs_lock(temp_mutex);
                    // cout<<"Node :"<<node_id<<" Waiting for log entry or sending heartbeat\n"<<endl;
                    cv_log_entry_or_heatbeat.wait_for(logs_lock,chrono::milliseconds(this->heartbeat_timer));
                    // cout<<this->trigger_tns<<endl;
                    if(this->stop_process||this->stop_election)break;
                    // if((!this->trigger_tns)){
                    if((true)){
                        // cout<<"Triggering Heartbeat"<<endl;
                        for (auto& [pid, stub] : this->stubs) {
                            if(pid==this->node_id) continue;
                            thread(&Node::send_heartbeat, this, pid).detach();
                        }
                        // cout<<"Ending Heartbeat"<<endl;
                    }
                    cout<<"Client Req Count:"<<this->client_reqs.size()<<endl;
                    // this->trigger_tns=false;
                    // cout<<"Node :"<<node_id<<" Waiting for log entry or sending heartbeat\n"<<endl;
                    // {
                    //     {
                    //         unique_lock<mutex> logs_lock(this->temp);
                    //         cv_log_entry_or_heatbeat.wait_for(logs_lock,chrono::seconds(15));
                    //     }
                    //     if((!this->trigger_tns)){
                    //         if(this->stop_process)break;
                    //         for (auto& [pid, stub] : this->stubs) {
                    //             if(pid==this->node_id) continue;
                    //             thread(&Node::send_heartbeat, this, pid).detach();
                    //         }
                    //     }
                    // }
                }
            }
            // if(this->stop_process){
                if(this->cur_leader==this->node_id)this->cur_leader=-1;
                this->is_leader=false;
                this->running_election=false;
                this->stop_election=false;
                this->election_completed=false;
                this->trigger_tns=false;
                this->votes=0;
                queue<Client_Req>().swap(this->client_reqs);
            // }
        }
        cout<<"Leader Failed"<<endl;
        if(!this->stop_process){
            // thread(&Node::reset_timer,this).detach();
            this->reset_timer();
        }
    }

    void send_prepare(int peer_id){
        Prepare req;
        req.set_type("PREPARE");
        Ballot* b = req.mutable_b(); // returns pointer to nested message
        b->set_ballot_num(this->ballot_num);
        b->set_sender_id(this->node_id);

        Acknowledge reply;
        reply.set_vote(false);
        ClientContext context;

        // cout<<"Sending.."<<endl;
        Status status = stubs[peer_id]->SendPrepare(&context, req, &reply);
        // cout<<"Sent"<<endl;

        // if (status.ok()) {
        // cout << "Node " << node_id << ": Successfully received reply from Node " << peer_id << endl;
        // } else {
        //     cerr << "Node " << node_id << ": RPC to Node " << peer_id << " failed. Error: " << status.error_code() << ": " << status.error_message() << endl;
        // }
        if(status.ok());
        else{
            return;
        }

        if(this->stop_process||!this->running_election||this->ballot_num>req.b().ballot_num())return;

        {
            unique_lock<mutex> lock(election);
            // cout<<"Node :"<<node_id<<" received reply from Node :"<<peer_id<<endl;

            // cout<<"Vote :"<<reply.vote()<<" with ballot number :"<<reply.b().ballot_num()<<" from Node :"<<reply.b().sender_id()<<endl;
            // cout<<this->stop_election<<endl;
            // cout<<status.ok()<<endl;
            // cout<<this->election_completed<<endl;

            if(!this->stop_election && status.ok()&&reply.vote()&&!this->election_completed){
                this->votes++;
                if (this->votes>=ceil(this->nodes_num / 2.0)) {
                    cout<<"Node : "<<node_id<<" Won election!\n"<<endl;
                    this->is_leader = true;
                    this->election_completed = true;
                    this->cur_leader=this->node_id;
                    cv_election.notify_all();


                    // vector<Log> new_view_logs;
                    // int cur_seq=1;
                    // int temp_ballot=1;
                    // int temp_sender_id=this->node_id;
                    // for (const Log& cur_log : this->logs) {
                    //     while(cur_seq<cur_log.seq_num){
                    //         this->new_view_logs.push_back(Log(pair<int,int>({temp_ballot,temp_sender_id}),cur_seq,pair<pair<string,string>,int>({pair<string,string>({"noop","noop"}),0})));
                    //         cur_seq++;
                    //     }

                    //     this->new_view_logs.push_back(cur_log);
                    //     cur_seq++;

                    //     temp_ballot=cur_log.b.first;
                    //     temp_sender_id=cur_log.b.second;
                    // }
                    // this->seq_num=max(this->seq_num,cur_seq);

                    // cout<<"Formed a temp NewViewLog"<<endl;
                }
            }

            // lock.unlock();
        }

        {
            unique_lock<mutex> log_lock(this->logs_mutex);

            if(!this->stop_election && status.ok()&&reply.vote()){  //Add functionality to create new view

                // int ctr=0;

                int cur_seq=1;
                int prev_size=this->logs.size();
                // this->logs.resize(max((int)this->logs.size(),reply.log()[reply.log().size()-1].seq_num()),Log());
                this->logs.resize(max((int)this->logs.size(),reply.log().size()),Log());
                // to access the last element

                // cout<<"Node :"<<node_id<<" Making its log from Node :"<<peer_id<<endl;
                for (const auto& peer_log : reply.log()) {

                    // if(peer_log.seq_num()==-1)continue;
                    if(cur_seq!=peer_log.seq_num());
                    if((this->execution_index>=cur_seq||this->commit_set.find(cur_seq)!=this->commit_set.end()));
                    else if(peer_log.tns().unique()==-1){
                        if(this->logs[cur_seq-1].seq_num==-1){

                            if(this->logs[cur_seq-1].seq_num!=-1){
                                this->tns_log.erase(this->logs[cur_seq-1].unique);
                                this->tns_to_client.erase(peer_log.tns().unique());
                            }
                            this->tns_log.insert(peer_log.tns().unique());
                            this->tns_to_client[peer_log.tns().unique()]=peer_log.tns().c().c1()[0]-'A'+1;

                            this->logs[peer_log.seq_num()-1]=Log(pair<int,int>({this->ballot_num,this->node_id}),cur_seq,pair<pair<string,string>,int>({pair<string,string>({"noop","noop"}),0}),-1);
                        }
                    }
                    else if(cur_seq>prev_size){
                        if(peer_log.tns().unique()!=-1){
                            if(this->logs[cur_seq-1].seq_num!=-1){
                                this->tns_log.erase(this->logs[cur_seq-1].unique);
                                this->tns_to_client.erase(peer_log.tns().unique());
                            }
                            this->tns_log.insert(peer_log.tns().unique());
                            this->tns_to_client[peer_log.tns().unique()]=peer_log.tns().c().c1()[0]-'A'+1;

                            this->logs[peer_log.seq_num()-1]=Log(pair<int,int>({this->ballot_num,this->node_id}),cur_seq,pair<pair<string,string>,int>({pair<string,string>({peer_log.tns().c().c1(),peer_log.tns().c().c2()}),peer_log.tns().amt()}),peer_log.tns().unique());
                        }
                        else{
                            if(this->logs[cur_seq-1].seq_num!=-1){
                                this->tns_log.erase(this->logs[cur_seq-1].unique);
                                this->tns_to_client.erase(peer_log.tns().unique());
                            }
                            this->logs[peer_log.seq_num()-1]=Log(pair<int,int>({this->ballot_num,this->node_id}),cur_seq,pair<pair<string,string>,int>({pair<string,string>({"noop","noop"}),0}),-1);
                        }
                        // continue;
                    }
                    else{
                        // cout<<"Node :"<<node_id<<" Making its log from Node :"<<peer_id<<"Checking seq : "<<peer_log.seq_num()<<endl; 
                        if((this->logs[cur_seq-1].tns.first.first=="noop"&&peer_log.tns().c().c1()!="noop")||this->logs[cur_seq-1].b.first<peer_log.b().ballot_num()){

                            this->tns_log.erase(this->logs[cur_seq-1].unique);
                            this->tns_log.insert(peer_log.tns().unique());

                            this->logs[cur_seq-1]=Log({peer_log.b().ballot_num(),peer_log.b().sender_id()},cur_seq,{{peer_log.tns().c().c1(),peer_log.tns().c().c2()},peer_log.tns().amt()},peer_log.tns().unique());
                        }
                    }
                    cur_seq++;


                    // while(cur_seq<peer_log.seq_num()){
                    //     if(cur_seq-1>=prev_size){
                    //         this->logs.push_back(Log(pair<int,int>({temp_ballot,temp_sender_id}),cur_seq,pair<pair<string,string>,int>({pair<string,string>({"noop","noop"}),0}),-1));
                    //     }
                    //     else if(this->logs[cur_seq-1].b.first<temp_ballot){
                    //         this->logs[cur_seq-1].b.first=temp_ballot;
                    //         this->logs[cur_seq-1].b.second=temp_sender_id;
                    //     }
                    //     cur_seq++;
                    // }
                    
                    // if(cur_seq-1>=prev_size){
                    //     this->logs.push_back(Log({peer_log.b().ballot_num(),peer_log.b().sender_id()},peer_log.seq_num(),{{peer_log.tns().c().c1(),peer_log.tns().c().c2()},peer_log.tns().amt()},peer_log.tns().unique()));
                    //     temp_ballot=peer_log.b().ballot_num();
                    //     temp_sender_id=peer_log.b().sender_id();
                    // }
                    // else{
                    //     temp_ballot=this->logs[cur_seq-1].b.first;
                    //     temp_sender_id=this->logs[cur_seq-1].b.second;
                    // }

                    // if(this->logs[cur_seq-1].b.first<peer_log.b().ballot_num()){
                    //     this->logs[cur_seq-1]=Log({peer_log.b().ballot_num(),peer_log.b().sender_id()},peer_log.seq_num(),{{peer_log.tns().c().c1(),peer_log.tns().c().c2()},peer_log.tns().amt()},peer_log.tns().unique());
                    //     temp_ballot=peer_log.b().ballot_num();
                    //     temp_sender_id=peer_log.b().sender_id();
                    // }
                    // else{
                    //     temp_ballot=this->logs[cur_seq-1].b.first;
                    //     temp_sender_id=this->logs[cur_seq-1].b.second;
                    // }
                    // new_view_logs.push_back(Log(reply.mutable_b().first()));
                    // cur_seq++;

                }
                seq_num_mutex.lock();
                this->seq_num=max(this->seq_num,cur_seq);
                seq_num_mutex.unlock();

                // cout<<"Node :"<<node_id<<" Updated its log from Node :"<<peer_id<<endl;
            }
        }
    }

    void send_newviewlog(int peer_id, shared_ptr<NewLogView> req, shared_ptr<map<int,int>> accepted_seq_count, shared_ptr<mutex> accepted_seq_count_mutex){
        Accepted reply;
        ClientContext context;

        // cout<<"Node :"<<node_id<<" sent New View Log to Node :"<<peer_id<<endl;


        // context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
        auto stream = stubs[peer_id]->SendNewLogView(&context, *req);
        if(this->stop_process)return;

        if(this->stop_process||!this->running_election||this->ballot_num>req->b().ballot_num())return;

        while (stream->Read(&reply)) {
            // cout<<"Node :"<<node_id<<" received Accepted for seq_num :"<<reply.seq_num()<<" from Node :"<<peer_id<<endl;
            if(this->stop_process)return;

            // this->logs_mutex.lock();
            accepted_seq_count_mutex->lock();
            commit_set_mutex.lock();
            if((*accepted_seq_count)[reply.seq_num()]!=-1){
                (*accepted_seq_count)[reply.seq_num()]++;

                if((*accepted_seq_count)[reply.seq_num()]>ceil(this->nodes_num/2)){
                    (*accepted_seq_count)[reply.seq_num()]=-1; //To avoid multiple commits for same seq_num

                    commit_set.insert(reply.seq_num());

                    for (auto& [pid, stub] : this->stubs) {
                        if(pid==this->node_id) continue;
                        thread(&Node::send_commit, this, pid, reply.seq_num()).detach();
                    }

                    cout<<"Leader :"<<this->node_id<<" Committed Command : "<<reply.seq_num()<<endl;

                    commit_tns_unique.insert(this->logs[reply.seq_num()-1].unique);
                    while(this->commit_set.find(this->execution_index+1)!=this->commit_set.end()){
                        // Write logic for executing commands, Handle out of order tns and "noop"
                        cout<<"Leader :"<<this->node_id<<" Executing Command : "<<this->execution_index<<endl;
                        if(this->logs[this->execution_index].tns.first.first=="noop")exec_status[this->logs[this->execution_index].seq_num]=true;
                        else{
                            bool success;
                            if(db[this->logs[this->execution_index].tns.first.first]>this->logs[this->execution_index].tns.second){
                                success=true;
                                db[this->logs[this->execution_index].tns.first.first]-=this->logs[this->execution_index].tns.second;
                                db[this->logs[this->execution_index].tns.first.second]+=this->logs[this->execution_index].tns.second;
                            }
                            else success=false;
                            
                            exec_status[this->logs[this->execution_index].seq_num]=success;
                            exec_tns_unique.insert(this->logs[this->execution_index].unique);
                            TransCompleted req;
                            Ack reply;
                            ClientContext context;
                            CompletionQueue cq;
                            Status status;
                            req.set_c1((int)(this->logs[this->execution_index].tns.first.first[0]-'A'+1));
                            req.set_unique(this->logs[this->execution_index].unique);
                            req.set_sender_id(this->node_id);
                            req.set_r(success);
                            // cout<<788<<endl;
                            // cout<<"Sending to :"<<(this->logs[this->execution_index].tns.first.first[0])<<endl;
                            // cout<<"Sending to :"<<(int)(this->logs[this->execution_index].tns.first.first[0]-'A')<<endl;
                            // std::unique_ptr<ClientAsyncResponseReader<Ack> > rpc(this->client_stubs[this->tns_to_client[this->logs[this->execution_index].unique]]->AsyncSendTransCompleted(&context,req,&cq));
                            std::unique_ptr<ClientAsyncResponseReader<Ack> > rpc(this->client_stubs[(int)(this->logs[this->execution_index].tns.first.first[0]-'A'+1)]->AsyncSendTransCompleted(&context,req,&cq));
                            
                            // cout<<355<<endl;
                            rpc->Finish(&reply, &status, (void*)nullptr);
                            // cout<<452<<endl;

                            cout<<"Leader Node :"<<this->node_id<<" Executed Command : "<<this->logs[this->execution_index].tns.first.first<<" to "<<this->logs[this->execution_index].tns.first.second<<" Amt : "<<this->logs[this->execution_index].tns.second<<endl;
                        }
                        this->execution_index++;
                    }
                    
                }
            }
            commit_set_mutex.unlock();
            accepted_seq_count_mutex->unlock();
        }
        cout<<"Node :"<<node_id<<" Completed New Log View for Peer :"<<peer_id<<endl;

            // this->logs_mutex.unlock();
    }

    void send_accept(int peer_id, Accept *req, shared_ptr<mutex>accepted_seq_count_mutex, shared_ptr<int>accepted_seq_val_count){
        Accepted reply;
        ClientContext context;

        Status status = stubs[peer_id]->SendAccept(&context, *req, &reply);

        if(this->stop_process)return;
        if(this->stop_process||!this->running_election||this->ballot_num>req->b().ballot_num())return;

        if(status.ok()) {
            // cout<<"Peer :"<<peer_id<<" waiting for mutex"<<endl;
            // unique_lock<mutex>lock(accepted_seq_count_mutex);

            (*accepted_seq_count_mutex).lock();
            if((*accepted_seq_val_count)!=-1){
                ((*accepted_seq_val_count))++;

                if((*accepted_seq_val_count)>ceil(this->nodes_num/2)){
                    (*accepted_seq_val_count)=-1; //To avoid multiple commits for same seq_num
                    
                    commit_set_mutex.lock();
                    commit_set.insert(reply.seq_num());
                    
                    for (auto& [pid, stub] : this->stubs) {
                        if(pid==this->node_id) continue;
                        thread(&Node::send_commit, this, pid, reply.seq_num()).detach();
                    }

                    // cout<<"Leader :"<<this->node_id<<" Committed Command : "<<reply.seq_num()<<endl;

                    commit_tns_unique.insert(this->logs[reply.seq_num()-1].unique);
                    while(this->commit_set.find(this->execution_index+1)!=this->commit_set.end()){
                        // cout<<"Found "<<this->execution_index+1<<" in commit_set"<<endl;
                        // Write logic for executing commands, Handle out of order tns and "noop"
                        if(this->logs[this->execution_index].tns.first.first=="noop")cout<<"Executing Noop"<<endl;
                        else{
                            bool success;
                            if(db[this->logs[this->execution_index].tns.first.first]>this->logs[this->execution_index].tns.second){
                                success=true;
                                db[this->logs[this->execution_index].tns.first.first]-=this->logs[this->execution_index].tns.second;
                                db[this->logs[this->execution_index].tns.first.second]+=this->logs[this->execution_index].tns.second;
                            }
                            else success=false;

                            exec_status[this->logs[this->execution_index].seq_num]=success;
                            exec_tns_unique.insert(this->logs[this->execution_index].unique);

                            // ClientContext context;
                            // TransCompleted req;
                            // Ack reply;
                            // Status status;

                            // unique_ptr<ClientAsyncResponseReader<Ack>> rpc;
                            TransCompleted req;
                            Ack reply;
                            ClientContext context;
                            CompletionQueue cq;
                            Status status;
                            req.set_c1((int)(this->logs[this->execution_index].tns.first.first[0]-'A'+1));
                            req.set_unique(this->logs[this->execution_index].unique);
                            req.set_sender_id(this->node_id);
                            req.set_r(success);

                            // std::unique_ptr<ClientAsyncResponseReader<Ack> > rpc(this->client_stubs[this->tns_to_client[this->logs[this->execution_index].unique]]->AsyncSendTransCompleted(&context,req,&cq));

                            std::unique_ptr<ClientAsyncResponseReader<Ack> > rpc(this->client_stubs[(int)(this->logs[this->execution_index].tns.first.first[0]-'A'+1)]->AsyncSendTransCompleted(&context,req,&cq));

                            rpc->Finish(&reply, &status, (void*)nullptr);

                            cout<<"Leader Node :"<<this->node_id<<" Executed Command : "<<this->logs[this->execution_index].tns.first.first<<" to "<<this->logs[this->execution_index].tns.first.second<<" Amt : "<<this->logs[this->execution_index].tns.second<<endl;
                        }
                        this->execution_index++;
                    }
                    commit_set_mutex.unlock();
                }
            }
            (*accepted_seq_count_mutex).unlock();
        }
        // cout<<"Leader exiting send_accept for peer id :"<<peer_id<<endl;
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

        Status status = stubs[peer_id]->SendCommit(&context, req, &reply);
        if(this->stop_process)return;
    }

    void send_heartbeat(int peer_id){

        Heartbeat req;
        Ack reply;
        ClientContext context;

        req.set_type("Heartbeat");
        req.set_sender_id(this->node_id);

        // cout<<"Node :"<<node_id<<" sent Heartbeat to Node :"<<peer_id<<endl;

        Status status = stubs[peer_id]->SendHeartbeat(&context, req, &reply);
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


    void dump_new_view() {
        logs_mutex.lock();
        ostream& os = cout;
        std::string filename = "view/logs_new_view.txt";

        // Clear file at start (truncate mode)
        std::ofstream outfile(filename, ios::app);

        while (!outfile.is_open()) {
            sleep(1);
        }

        outfile << "\n================= New View with "<<this->ballot_num<<" =================\n";
        if (logs.empty()) {
            outfile << "(No logs available)\n";
        } 
        else {
            for (size_t i = 0; i < this->logs.size(); i++) {
                const auto& log = this->logs[i];
                outfile << "Log[" << std::setw(2) << i << "]\n";
                outfile << "  Ballot     : (" << log.b.first << ", " << log.b.second << ")\n";
                outfile << "  SeqNum     : " << log.seq_num << "\n";
                outfile << "  Transaction: " 
                << log.tns.first.first << " -> " << log.tns.first.second 
                << " | Amt=" << log.tns.second << "\n";
                outfile << "--------------------------------------------\n";
            }
        }
        outfile << "============================================\n\n";
        logs_mutex.unlock();
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

    // void clear_common_log_file() {
    //     fs::create_directories("logs"); // make sure folder exists

    //     std::ofstream outfile("logs/all_nodes_log.txt", std::ios::trunc);
    //     outfile << "========== New Run Log ==========\n";
    //     outfile.close();
    // }

    void forward_to_leader(ClientTransaction *req) {
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

    Status SendPrepare(ServerContext*c, const Prepare*req, Acknowledge*reply) override {
        if(this->stop_process)return Status::CANCELLED;
        unique_lock<mutex> lock_vote(vote_mutex);
        unique_lock<mutex>lock(processing_msg);
        if(this->stop_process)return Status::CANCELLED;
        reply->set_vote(false);
        if(this->ballot_num<req->b().ballot_num()) {

            if(this->is_leader){
                this->is_leader=false;
                this->stop_election=true;
                this->cur_leader=req->b().sender_id();
                cout<<"Node "<<this->node_id<<" Stepping Down"<<endl;
            }

            Ballot* b = reply->mutable_b();

            this->ballot_num=req->b().ballot_num();
            this->cur_leader=req->b().sender_id();
            this->election_map[this->ballot_num]=req->b().sender_id();

            b->set_ballot_num(this->ballot_num);
            b->set_sender_id(this->cur_leader);

            reply->set_vote(true);


            cout<<"Node :"<<this->node_id<<" Voted for :"<<req->b().sender_id()<<endl;
            
            // reply->set_log(this->logs); //Correct this as it cannot directly take vector
            for (auto &log : this->logs){
                LogMsg *temp=reply->add_log();
                Ballot* temp_b = temp->mutable_b();
                temp_b->set_ballot_num(log.b.first);
                temp_b->set_sender_id(log.b.second);
                temp->set_seq_num(log.seq_num);
                Transaction* temp_tns = temp->mutable_tns();
                Clients* temp_c = temp_tns->mutable_c();
                temp_c->set_c1(log.tns.first.first);
                temp_c->set_c2(log.tns.first.second);
                temp_tns->set_amt(log.tns.second);
                temp_tns->set_unique(log.unique);
            }
            // cout<<"Node :"<<this->node_id<<" Vote Sent to :"<<req->b().sender_id()<<endl;
            this->reset_timer();
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
            // cout<<"Node :"<<node_id<<" Updated its log from Node :"<<req->b().sender_id()<<" for all seq_nums"<<endl;
            this->reset_timer();  // Commented For testing
        }
        return Status::OK;
    }

    Status SendAccept(ServerContext*c, const Accept*req, Accepted*reply) override{
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

            this->ballot_num=req->b().ballot_num();
            Ballot* b = reply->mutable_b();
            
            b->set_ballot_num(this->ballot_num);
            b->set_sender_id(this->cur_leader);
            reply->set_seq_num(req->seq_num());
            // if(this->node_id==1) cout<<"Node :"<<node_id<<" Got Accept from Node :"<<req->b().sender_id()<<" for seq_num :"<<req->seq_num()<<endl;
            this->logs.resize(max((int)this->logs.size(),req->seq_num()),Log());


            if(this->logs[req->seq_num()-1].seq_num!=-1){
                this->tns_log.erase(this->logs[req->seq_num()-1].unique);
                this->tns_to_client.erase(this->logs[req->seq_num()-1].unique);
            }
            this->tns_log.insert(req->tns().unique());
            this->tns_to_client[req->tns().unique()]=req->tns().c().c1()[0]-'A'+1;

            Log temp;
            temp.b={req->b().ballot_num(),req->b().sender_id()};
            temp.seq_num=req->seq_num();
            temp.tns={{req->tns().c().c1(),req->tns().c().c2()},req->tns().amt()};
            temp.unique=req->tns().unique();
            logs_mutex.lock();
            // if(this->node_id==1)cout<<"Node :"<<node_id<<" Updated its log from Node :"<<req->b().sender_id()<<" for seq_num :"<<req->seq_num()<<endl;
            // logs.push_back(temp);
            logs[req->seq_num()-1] = temp;
            logs_mutex.unlock();
            this->seq_num=max(this->seq_num,req->seq_num()+1);

            this->reset_timer();  // Commented for Testing
        }
        return Status::OK;
    }

    Status SendCommit(ServerContext*c, const Commit*req, Ack*reply) override{
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

            if(req->b().ballot_num()!=this->logs[req->seq_num()-1].b.first){
                // cout<<"Node :"<<this->node_id<<" cannot Commit for seq_num :"<<req->seq_num()<<" as no msg exist"<<endl;
                return Status::OK;
            }
            // cout<<"Node :"<<this->node_id<<" Commit for seq_num :"<<req->seq_num()<<endl;

            commit_set_mutex.lock();
            commit_set.insert(req->seq_num());
            commit_tns_unique.insert(this->logs[req->seq_num()-1].unique);
            while(this->commit_set.find(this->execution_index+1)!=this->commit_set.end()){
                // Write logic for executing commands, Handle out of order tns and "noop"
                if(this->logs[this->execution_index].tns.first.first=="noop");
                else{
                    bool success;
                    if(db[this->logs[this->execution_index].tns.first.first]>this->logs[this->execution_index].tns.second){
                        success=true;
                        db[this->logs[this->execution_index].tns.first.first]-=this->logs[this->execution_index].tns.second;
                        db[this->logs[this->execution_index].tns.first.second]+=this->logs[this->execution_index].tns.second;
                    }
                    else success=false;

                    exec_status[this->logs[this->execution_index].seq_num]=success;
                    exec_tns_unique.insert(this->logs[this->execution_index].unique);

                    // cout<<"Node :"<<this->node_id<<" Executed Command : "<<this->logs[this->execution_index].tns.first.first<<" to "<<this->logs[this->execution_index].tns.first.second<<" Amt : "<<this->logs[this->execution_index].tns.second<<endl;
                }
                this->execution_index++;
            }
            commit_set_mutex.unlock();    
        }

        this->reset_timer();
        return Status::OK;
    }

    Status SendHeartbeat(ServerContext*c, const Heartbeat*req, Ack*reply) override{
        if(this->stop_process)return Status::CANCELLED;
        unique_lock<mutex>lock(processing_msg);
        if(this->cur_leader==-1||this->cur_leader==req->sender_id()){
            // cout<<"Node :"<<node_id<<" received Heartbeat from Node :"<<req->sender_id()<<endl;
            // if(node_id==1) cout<<"Node :"<<node_id<<" received Heartbeat from Node :"<<req->sender_id()<<endl;
            this->cur_leader=req->sender_id();

            this->reset_timer(); //Commented for Testing
        }
        return Status::OK;
    }

    // Status SendClientTransaction(ServerContext*c, const ClientTransaction*req, TransAck*reply) override{
    //     if(this->stop_process)return Status::CANCELLED;

    //     if(this->cur_leader==this->node_id){
    //         this->client_reqs.push(Client_Req({{req->tns().c().c1(),req->tns().c().c2()},req->tns().amt()},req->c1(),(int)req->unique()));
    //         cout<<"Node :"<<node_id<<" received Client Transaction from Client Tns_id :"<<req->unique()<<endl;
    //     }
    //     // else{
    //     //     this->stubs[this->cur_leader]->grpc::SendClientTransaction(c, *req, reply);
    //     // }
    //     return Status::OK;
    // }

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
            // unique_lock<mutex> lock_process(* new mutex());
            // while(true){
            //     this->cv_stop_process.wait_for(lock_process,process_stop_timer); //wait till some signal is received to resume
            //     if(!this->stop_process) break;
            // }
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
        else if(req->type()=="PrintLog"){
            this->dump_logs();
        }
        else if(req->type()=="PrintDB"){
            this->dump_db();
        }
        // else if(req->type()=="PrintView"){
        //     this->dump_();
        // }
        else if(req->type()=="PrintStatus"){
            int x=req->seq_num();
            ;
            if(x>=this->seq_num){
                cout<<"For Node :"<<this->node_id<<" Status of Seq Num "<<x<<" is X"<<endl;
            }
            else if(this->execution_index>=x){
                cout<<"For Node :"<<this->node_id<<" Status of Seq Num "<<x<<" is E"<<endl;
            }
            else if(this->commit_set.find(x)!=this->commit_set.end()){
                cout<<"For Node :"<<this->node_id<<" Status of Seq Num "<<x<<" is C"<<endl;
            }
            else{
                cout<<"For Node :"<<this->node_id<<" Status of Seq Num "<<x<<" is A"<<endl;
            }
        }
        else if(req->type()=="ResumeProcess"){
            this->stop_process=false;
            this->stop_election=false;
            this->reset_timer();
            // cv_stop_process.notify_all();
            // thread(&Node::start_timer, this).detach();
            reply->set_type("Process Resumed");
            cout<<"Node :"<<this->node_id<<" Process Resumed\n"<<endl;
        }
        return Status::OK;
    }

    private:
    // Class encompassing the state and logic needed to serve a request.
    class AsyncSendClientTransactionCall {
    public:
        // Take in the "service" instance (in this case representing an asynchronous
        // server) and the completion queue "cq" used for asynchronous communication
        // with the gRPC runtime.
        AsyncSendClientTransactionCall(NodeService::AsyncService* service, ServerCompletionQueue* cq, Node* node)
            : service_(service), cq_(cq), responder_(&ctx_), node_(node), status_(CREATE) {
        // Invoke the serving logic right away.
        // cout<<"1"<<endl;
        Proceed();
        }

        void Proceed() {
            // cout<<"2"<<endl;
            if (status_ == CREATE) {
                // Make this instance progress to the PROCESS state.
                status_ = PROCESS;

                // As part of the initial CREATE state, we *request* that the system
                // start processing SayHello requests. In this request, "this" acts are
                // the tag uniquely identifying the request (so that different CallData
                // instances can serve different requests concurrently), in this case
                // the memory address of this CallData instance.
                // cout<<3<<endl;
                service_->RequestSendClientTransaction(&ctx_, &req_, &responder_, cq_, cq_,this);
                // cout<<4<<endl;
            } 
            else if (status_ == PROCESS) {
                // Spawn a new CallData instance to serve new clients while we process
                // the one for this CallData. The instance will deallocate itself as
                // part of its FINISH state.
                // cout<<5<<endl;
                // if(node_->stop_process)return;

                new AsyncSendClientTransactionCall(service_, cq_, node_);

                if(!node_->stop_process){
                    {
                        // Wait for a leader with a predicate and proper lock usage:
                        // cout<<"Waiting Election"<<endl;
                        std::unique_lock<std::mutex> lock(node_->election);
                        node_->cv_election.wait_for(lock, std::chrono::seconds(5),
                        [&](){ return node_->cur_leader != -1; });  
                        // cout<<"Election Ended"<<endl;
                        // lock.unlock();
                        // lock is released when it goes out of scope
                    }

                    unique_lock<mutex> lock(node_->form_new_log_view_mutex); 
                    if (node_->cur_leader == node_->node_id) {
                        // push the client request into local queue
                        if(node_->tns_log.find(req_.unique())==node_->tns_log.end()&&node_->tns_to_client.find(req_.unique())==node_->tns_to_client.end()){
                            node_->tns_to_client[req_.unique()] = req_.c1();
                            // std::lock_guard<std::mutex> lg(node_->logs_mutex); // or some suitable mutex
                            // node_->tns.insert(req_.unique());
                            node_->tns_log.insert(req_.unique());
                            node_->client_reqs.push(Client_Req(
                                {{req_.tns().c().c1(), req_.tns().c().c2()}, req_.tns().amt()},
                                req_.c1(), (int)req_.unique()));
                                // std::cout << "Node :" << node_->node_id<< " received Client Transaction Tns_id :"<< req_.unique() << std::endl;
                        }
                        else if(node_->exec_status.find(req_.unique())!=node_->exec_status.end()){
                            ClientContext context;
                            TransCompleted req;
                            CompletionQueue cq;
                            req.set_c1(req_.c1());
                            req.set_unique(req_.unique());
                            req.set_sender_id(node_->node_id);
                            req.set_r(node_->exec_status[req_.unique()]);
                            Ack reply;
                            Status status;
                            std::unique_ptr<ClientAsyncResponseReader<Ack> > rpc(node_->client_stubs[req_.c1()]->AsyncSendTransCompleted(&context,req,&cq));

                            rpc->Finish(&reply, &status, (void*)nullptr);
                        }
                        else{
                            // std::cout << "Node :" << node_->node_id<< " received Duplicate Client Transaction Tns_id :"<< req_.unique() << std::endl;
                        }
                    } 
                    else {
                        // std::cout << "Node :" << node_->node_id << " Forwarding to Leader :" << node_->cur_leader << std::endl;
                        // forward_to_leader must copy req into heap and use shared CQ or forward CQ safely
                        // node_->tns_to_client[req_.unique()] = req_.c1();
                        if(node_->cur_leader!=-1)node_->forward_to_leader(&req_);
                    }
                }

                status_ = FINISH;
                // sleep(3);
                responder_.Finish(reply_, Status::OK, this);
            } 
            else {
                // cout<<6<<endl;
                // CHECK_EQ(status_, FINISH);
                // Once in the FINISH state, deallocate ourselves (CallData).
                delete this;
            }
        }

    private:
        // The means of communication with the gRPC runtime for an asynchronous
        // server.
        NodeService::AsyncService* service_;
        // The producer-consumer queue where for asynchronous server notifications.
        ServerCompletionQueue* cq_;
        // Context for the rpc, allowing to tweak aspects of it such as the use
        // of compression, authentication, as well as to send metadata back to the
        // client.
        ServerContext ctx_;

        Node* node_;

        // What we get from the client.
        ClientTransaction req_;
        // What we send back to the client.
        TransAck reply_;

        // The means to get back to the client.
        ServerAsyncResponseWriter<TransAck> responder_;

        // Let's implement a tiny state machine with the following states.
        enum CallStatus { CREATE, PROCESS, FINISH };
        CallStatus status_;  // The current serving state.
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
    thread client_server;
    chrono::milliseconds client_timer;
    std::mt19937 rng;
    std::uniform_int_distribution<int> unique_dist;

    // thread async_server_thread_;
    // msg::NodeService::AsyncService service_;
    // std::unique_ptr<ServerCompletionQueue> cq_;
    // std::thread server_thread_;

public:
    Client(int client_id,int nodes_num) {
        this->service_ = new msg::NodeService::AsyncService();
        this->client_id=client_id;
        this->leader=1;
        this->nodes_num=nodes_num;
        this->client_timer=chrono::milliseconds(600);
        std::random_device rd;
        this->rng.seed(rd() ^ (static_cast<unsigned>(this->client_id) << 16));
        this->unique_dist = std::uniform_int_distribution<int>(1, std::numeric_limits<int>::max());
        // async_server_thread_ = thread(&Client::AsyncCompleteRpc, this);
        // if(client_id==1){
        //     cout<<"Created Client 1"<<endl;
        //     this->client_reqs.push(Client_Req({{"A","B"},10},1,-1));
        // }
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
        // cout << "[" << client_id << "] Async Server listening on " << address << endl;

        // Create the initial handler that arms the RPC
        // new AsyncClientTransactionCall(service_, cq_.get(), this);

        // Start the CQ processing thread (do not detach)
        // async_server_thread_ = std::thread([this]() { this->HandleRpcs(); });
        HandleRpcs();
        // Option: block here or return — if you want run_server to block, you can call server_->Wait()
        // server_->Wait(); // if you want this thread to block here
    }

    // class CallData {
    // public:
    //     virtual void Proceed(bool ok) = 0;
    //     virtual ~CallData() {}
    // };

    void HandleRpcs() {
    // Spawn a new CallData instance to serve new clients.
        new AsyncTransCompletedCall(service_, cq_.get(),this);
        void* tag;  // uniquely identifies a request.
        bool ok;
        while (true) {
        // Block waiting to read the next event from the completion queue. The
        // event is uniquely identified by its tag, which in this case is the
        // memory address of a CallData instance.
        // The return value of Next should always be checked. This return value
        // tells us whether there is any kind of event or cq_ is shutting down.
        //   CHECK(cq_->Next(&tag, &ok));
        //   CHECK(ok);
            // cout<<"Waiting for event"<<endl;
            cq_->Next(&tag, &ok);
            // cout<<"Got new event"<<endl;
            static_cast<AsyncTransCompletedCall*>(tag)->Proceed();
        }
        cout<<"Ending"<<endl;
    }

    void send_node(){
        unique_lock<mutex> lock(queue_reqs);
        while(true){
            if(!this->client_reqs.empty()){
                // Logic to send to node

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
                // context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

                if(this->leader != -1){
                    TransAck reply;
                    ClientContext context;
                    CompletionQueue cq;
                    Status status;
                    // grpc::CompletionQueue cq;
                    // auto rpc = node_stubs[this->leader]->AsyncSendClientTransaction(&context, req, &reply);
                    // rpc->Finish(, new Status, (void*)1);
                    // Status status = node_stubs[this->leader]->SendClientTransaction(&context, req, &reply);
                    // cout<<"Client :"<<this->client_id<<" Sending Tns to Node :"<<this->leader<<endl;
                    // this->node_stubs[this->leader]->AsyncSendClientTransaction(&context, req, &cq);
                    std::unique_ptr<ClientAsyncResponseReader<TransAck> > rpc(this->node_stubs[this->leader]->AsyncSendClientTransaction(&context, req, &cq));

                    rpc->Finish(&reply, &status, (void*)nullptr);
                    // call->response_reader->StartCall();
                    // if(status.ok()){
                    //     if(reply.unique()==client_reqs.front().unique){
                    //         cout<<"Client :"<<this->client_id<<" Got Msg Exectued Tns :"<<this->leader<<endl;
                    //     }
                    // }
                    this->leader=-1;
                }
                else{
                    // cout<<"Client :"<<this->client_id<<" Sending Tns to All Nodes"<<endl;
                    for (int k=1;k<=this->nodes_num;k++){
                        TransAck reply;
                        ClientContext context;
                        CompletionQueue cq;
                        Status status;
                        // this->node_stubs[this->leader]->AsyncSendClientTransaction(&context, req, &cq);
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

        if(req->msg()=="start"){
            cv_queue_reqs.notify_one();
            return Status::OK;
        }

        unique_lock<mutex> lock(queue_reqs);
        // this->client_reqs.push(Client_Req({{req->tns().c().c1(),req->tns().c().c2()},req->tns().amt()},req->c1(),req->unique()));
        this->client_reqs.push(Client_Req({{req->tns().c().c1(),req->tns().c().c2()},req->tns().amt()},req->c1(),-1));
        // cout<<"Client :"<<this->client_id<<" received Transaction"<<endl;
        lock.unlock();
        // cv_queue_reqs.notify_one();
        return Status::OK;
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
            // Arm: ask gRPC to notify us into `cq` when a SendTransCompleted arrives.
            // DO NOT call Proceed here and DO NOT new another object here.
        }
        
        void Proceed() {
            if (status == CREATE) {
                // cout<<12<<endl;
                // An RPC has arrived for this object (tagged). Spawn a new listener
                // to keep the service continuously available.
                
                // Move into processing phase for this request
                
                service->RequestSendTransCompleted(&ctx, &request, &responder, cq, cq, this);
                status = PROCESS;
                // If 'ok' is false, the RPC was cancelled; cleanup.
                // if (!ok) { delete this; return; }
                
                // FALL THROUGH to PROCESS handling (or return and expect another Next() event
                // which will call Proceed again — both styles exist; below we handle processing now)
            }
            
            else if (status == PROCESS) {
                // cout<<13<<endl;
                
                // Process the request quickly while holding minimal locks
                {
                    std::lock_guard<std::mutex> lk(client->queue_reqs);
                    client->leader = request.sender_id();
                    if (!client->client_reqs.empty() &&
                    request.unique() == client->client_reqs.front().unique) {
                        client->client_reqs.pop();
                        if(request.r()) std::cout << "Client " << client->client_id<< " confirmed successful execution of transaction Unique_id="<< request.unique() << std::endl;
                        else std::cout << "Client " << client->client_id<< " confirmed failed execution of transaction Unique_id="<< request.unique() << std::endl;
                    }
                }
                client->cv_queue_reqs.notify_one();
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
map<int, vector<pair<pair<string,string>, int>>> set_transactions;
map<int, vector<bool>> set_live_nodes;

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
    getline(file, line); // skip header
    int prev_set = -1;

    while (getline(file, line)) {
        // --- Split respecting quotes ---
        vector<string> cols;
        string cur;
        bool in_quotes = false;
        for (char ch : line) {
            if (ch == '"') in_quotes = !in_quotes;
            else if (ch == ',' && !in_quotes) {
                cols.push_back(cur);
                cur.clear();
            } else cur.push_back(ch);
        }
        cols.push_back(cur);

        if (cols.size() < 3) continue;

        string set_str = cols[0];
        string txn_str = cols[1];
        string nodes_str = cols[2];

        int set_number;
        if (set_str.empty()) set_number = prev_set;
        else {
            set_number = stoi(set_str);
            prev_set = set_number;
        }


        // trim
        auto trim = [](string& s) {
            s.erase(remove_if(s.begin(), s.end(), ::isspace), s.end());
        };

        // --- Handle LF ---
        if (txn_str.find("LF") != string::npos) {
            set_transactions[set_number].push_back({{"LF", "LF"}, 0});
            // continue;
        }
        else {

            // --- Clean transaction string ---
            txn_str.erase(remove(txn_str.begin(), txn_str.end(), '('), txn_str.end());
            txn_str.erase(remove(txn_str.begin(), txn_str.end(), ')'), txn_str.end());
            txn_str.erase(remove(txn_str.begin(), txn_str.end(), '"'), txn_str.end());
            
            stringstream txn_ss(txn_str);
            string from, to, amt_str;
            getline(txn_ss, from, ',');
            getline(txn_ss, to, ',');
            getline(txn_ss, amt_str, ',');
            
            trim(from); trim(to); trim(amt_str);
            
            if (!amt_str.empty()) {
                int amt = stoi(amt_str);
                set_transactions[set_number].push_back({{from, to}, amt});
            }
        }

        // --- Parse live nodes ---
        nodes_str.erase(remove(nodes_str.begin(), nodes_str.end(), '['), nodes_str.end());
        nodes_str.erase(remove(nodes_str.begin(), nodes_str.end(), ']'), nodes_str.end());
        trim(nodes_str);
        if(nodes_str.empty()) continue;
        vector<bool> nodes(5,false);
        stringstream nodes_ss(nodes_str);
        string node;
        while (getline(nodes_ss, node, ',')) {
            node.erase(remove(node.begin(), node.end(), 'n'), node.end());
            trim(node);
            if (!node.empty())
                nodes[node[0]-'1']=true;
        }
        set_live_nodes[set_number] = nodes;
    }
}


int main(){
    signal(SIGINT, handle_sigint);


    std::ofstream outfile("view/logs_new_view.txt", std::ios::out | std::ios::trunc);
    outfile<<"";
    outfile.close();

    map<int,shared_ptr<msg::NodeService::Stub>> node_stubs;
    map<int,shared_ptr<msg::NodeService::Stub>> client_stubs;
    read_batches_from_csv("testcase.csv");
    map<string,int> db;
    int n_nodes = 5;
    int n_clients=10;
    int set_num=10;
    vector<bool> cur_live(n_nodes,true);
    for(char x='A';x<='J';x++){
        db.emplace(string(1,x),10);
        // cout<<string(1,x)<<" "<<10<<endl;
    }

    // bool req_live[n_nodes];

    // cout << "\n--- Parsed Transactions ---\n";
    // for (const auto& [set_id, txns] : set_transactions) {
    //     cout << "Set " << set_id << ":\n";
    //     for (const auto& t : txns) {
    //         cout << "  (" << t.first.first << ", " << t.first.second << ", " << t.second << ")\n";
    //     }
    // }

    // cout << "\n--- Live Nodes ---\n";
    // for (const auto& [set_id, nodes] : set_live_nodes) {
    //     cout << "Set " << set_id << ": ";
    //     for (bool n : nodes) cout << "n" << n << " ";
    //     cout << "\n";
    // }
    // cout << "\n---Cur Live Nodes ---\n";
    // cout << "Set : ";
    // for (bool x : cur_live) {
    //     cout << "n" << x << " ";
    // }
    // cout << "\n";

    for (int node_id = 1; node_id <= n_nodes; ++node_id) {
        pid_t pid = fork();
        if(pid==0){
            // Node node(stubs, node_id,n);  
            Node node(node_id,n_nodes,db,n_clients);  
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

        // else if(choice==4){
        //     Command req;
        //     req.set_type("PrintView");
        //     for(int i=1;i<=n_nodes;i++){
        //         Ack reply;
        //         ClientContext context;
        //         Status status = node_stubs[i]->ActionCommand(&context, req, &reply);
        //     }
        // }

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
            for (auto& tns:set_transactions[set_num]){

                if(tns.first.first=="LF"){
                    int temp=-1;
                    cout<<"LF Detected. Press 6 to proceed with LF"<<endl;
                    while(true){
                        sleep(1);
                        Command req;
                        req.set_type("TriggerTns");
                        for(int i=1;i<=n_nodes;i++){
                            Ack reply;
                            ClientContext context;
                            Status status = node_stubs[i]->ActionCommand(&context, req, &reply);
                        }
                        cin>>temp;
                        if(temp==6) {
                            cout<<"Proceeding LF"<<endl;

                            Command req;
                            req.set_type("LF");
                            for(int i=1;i<=n_nodes;i++){
                                Ack reply;
                                ClientContext context;
                                Status status = node_stubs[i]->ActionCommand(&context, req, &reply);
                                if(status.ok()&&reply.type()=="Leader"){
                                    cur_live[i-1]=false;
                                }
                            }

                            break;
                        }
                        // if(temp==1){
                        //     Command req;
                        //     req.set_type("TriggerTns");
                        //     for(int i=1;i<=n_nodes;i++){
                        //         Ack reply;
                        //         ClientContext context;
                        //         Status status = node_stubs[i]->ActionCommand(&context, req, &reply);
                        //     }
                        // }
                        else{
                            cout<<"Invalid input. Press 6 to proceed with LF"<<endl;
                        }
                    }
                    cout<<"Wait for Msg Leader Failed and Press Enter"<<endl;
                    cin.ignore();
                    cin.get();
                    continue;
                }

                ClientTransaction req;
                TransAck reply;
                ClientContext context;

                req.set_c1(tns.first.first[0]);
                Transaction* tns_ptr = req.mutable_tns();
                Clients* c = tns_ptr->mutable_c();
                c->set_c1(tns.first.first);
                c->set_c2(tns.first.second);
                tns_ptr->set_amt(tns.second);
                Status status = client_stubs[tns.first.first[0]-64]->SendClientTransaction(&context, req, &reply);
                // cout<<"Main Sent to "<<tns.first.first[0]-64<<endl;
            }
            for(int i=1;i<=n_clients;i++){
                ClientTransaction req;
                TransAck reply;
                ClientContext context;

                req.set_msg("start");
                Status status = client_stubs[i]->SendClientTransaction(&context, req, &reply);
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