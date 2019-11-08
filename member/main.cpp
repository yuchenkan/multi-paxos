#include <unistd.h>
#include <stdlib.h>

#include <set>

#include "indet.h"
#include "paxos.h"

class AppliedMembershipChanges
{
public:
    AppliedMembershipChanges(SpinLock * lock)
        : lock_(lock) { }

    void Insert(const std::string & change, const Thread * thread)
    {
        lock_->Lock(thread);
        changes_.insert(change);
        lock_->Unlock();
    }

    bool Contain(const std::string & change, const Thread * thread)
    {
        bool ret;
        lock_->Lock(thread);
        ret = changes_.find(change) != changes_.end();
        lock_->Unlock();
        return ret;
    }

private:
    SpinLock * lock_;
    std::set<std::string> changes_;
};

class Callback : public paxos::Callback
{
public:
    Callback(Logger * logger, AppliedMembershipChanges * applied_membership_changes)
        : logger_(logger), applied_membership_changes_(applied_membership_changes) { }

    virtual void Unproposable(Thread * thread, const std::string & cb)
    {
        //WARNING(logger_, thread, "unproposable: %s\n", cb.c_str());
    }

    virtual void Accepted(Thread * thread, const std::string & cb)
    {
        INFO(logger_, thread, "accepted: %s\n", cb.c_str());
    }

    virtual void Applied(Thread * thread, const std::string & cb, const std::string * result)
    {
        INFO(logger_, thread, "applied: %s\n", cb.c_str());
        if (strncmp(cb.c_str(), "member", strlen("member")) == 0)
            applied_membership_changes_->Insert(cb, thread);
    }

private:
    Logger * logger_;

    AppliedMembershipChanges * applied_membership_changes_;
};

class NetWork : public paxos::NetWork
{
public:
    NetWork(Logger * logger, NetWork ** nets) : logger_(logger), nets_(nets) { }

    virtual void Send(Thread * thread, paxos::NodeID node, const std::string & msg)
    {
        INFO(logger_, thread, "send to %u\n", node);
        nets_[node]->OnReceive(msg, thread);
    }

private:
    Logger * logger_;
    NetWork ** nets_;
};

class StateMachine : public paxos::StateMachine
{
public:
    StateMachine(Logger * logger, Queue<unsigned int> * queue)
        : logger_(logger), queue_(queue) { }

    virtual bool Apply(Thread * thread, const std::string & value, std::string * result)
    {
        INFO(logger_, thread, "apply: %s\n", value.c_str());
        queue_->Put(FromStr<unsigned int>(value), thread);
        return false;
    }

private:
    Logger * logger_;
    Queue<unsigned int> * queue_;
};

static void * RunMembershipChanges(void * arg);

class MembershipChanges
{
public:
    MembershipChanges(Logger * logger, Thread * thread,
            AppliedMembershipChanges * changes, paxos::Node * node, unsigned int srvcnt)
        : logger_(logger), changes_(changes), node_(node), srvcnt_(srvcnt),
            down_(thread->NewAtomicBool(false)), thread_(NULL)
    {
        thread->NewThread(&thread_, RunMembershipChanges, this, "member");
    }

    ~MembershipChanges()
    {
        delete down_;
        thread_->Join();
        delete thread_;
    }

    bool Down(const Thread * thread) { return down_->Get(thread); }

    void Loop()
    {
        for (unsigned int i = 0; i < srvcnt_ * 2; ++i)
        {
            if (i % srvcnt_ != 0)
            {
                if (i / srvcnt_ % 2 == 0)
                {
                    INFO(logger_, thread_, "add acceptor %u\n", i % srvcnt_);
                    node_->AddAcceptor(i % srvcnt_, "member " + ToStr(i), thread_);
                }
                else
                {
                    INFO(logger_, thread_, "del acceptor %u\n", i % srvcnt_);
                    node_->DelAcceptor(i % srvcnt_, "member " + ToStr(i), thread_);
                }

                while (!changes_->Contain("member " + ToStr(i), thread_))
                    thread_->USleep(1000);

                thread_->Sleep(10);
            }
        }

        down_->Set(true, thread_);
    }

private:
    Logger * logger_;
    AppliedMembershipChanges * changes_;
    paxos::Node * node_;
    unsigned int srvcnt_;
    AtomicBool * down_;
    Thread * thread_;
};

static void * RunMembershipChanges(void * arg)
{
    ((MembershipChanges *)arg)->Loop();
    return NULL;
}

int main(int argc, const char ** argv)
{
    assert(argc == 6);
    unsigned int srvcnt = (unsigned int)atoi(argv[1]);
    assert(srvcnt <= 32);
    unsigned int interval = (unsigned int)atoi(argv[2]);
    unsigned long long failure_rate = (unsigned long long)atoi(argv[3]);
    const char * dir = argv[4];
    bool replay = strcmp(argv[5], "true") == 0;

    Indet * indet = new Indet("main", dir, replay);
    int seed = (int)indet->GetClock()->Now(indet->GetMainThread());
    indet->GetMainThread()->StartRandomFailure(seed + srvcnt, failure_rate);
    SpinLock * logger_lock = indet->GetMainThread()->NewSpinLock();
    Logger * logger = new Logger(logger_lock, indet->GetClock(), 0);
    AppliedMembershipChanges * changes = new AppliedMembershipChanges(indet->GetMainThread()->NewSpinLock());
    Timer * timers[srvcnt];
    Rand * rands[srvcnt];
    Callback * cbs[srvcnt];
    NetWork * nets[srvcnt];
    SpinLock * queue_locks[srvcnt];
    Queue<unsigned int> * queues[srvcnt];
    StateMachine * sms[srvcnt];
    paxos::Node * nodes[srvcnt];
    for (unsigned int i = 0; i < srvcnt; ++i)
    {
        timers[i] = new Timer(logger);
        rands[i] = new Rand(seed + i);
        cbs[i] = new Callback(logger, changes);
        nets[i] = new NetWork(logger, nets);
        queue_locks[i] = indet->GetMainThread()->NewSpinLock();
        queues[i] = new Queue<unsigned int>(queue_locks[i]);
        sms[i] = new StateMachine(logger, queues[i]);
        nodes[i] = new paxos::Node(indet->GetMainThread(), i, 0,
                                            logger, indet->GetClock(), timers[i], rands[i],
                                            cbs[i], nets[i], sms[i], paxos::Config());
        nodes[i]->Init(ToStr(i), indet->GetMainThread());
    }

    MembershipChanges * membership_changes = new MembershipChanges(logger, indet->GetMainThread(), changes, nodes[0], srvcnt);

    unsigned int first = 0;
    for (unsigned int i = 0; !membership_changes->Down(indet->GetMainThread()); ++i)
    {
        if (i % srvcnt == 0) ++first;
        nodes[i % srvcnt]->Propose(ToStr(i), ToStr(i), indet->GetMainThread());

        if (i % srvcnt == srvcnt - 1)
            indet->GetMainThread()->USleep(interval * srvcnt);
    }

    delete membership_changes;

    std::set<unsigned int> first_applied;
    std::vector<unsigned int> results[srvcnt];
    while (first_applied.size() != first)
    {
        unsigned int a;
        if (queues[0]->Get(&a, indet->GetMainThread()))
        {
            if (a % srvcnt == 0)
                assert(first_applied.insert(a).second);
            results[0].push_back(a);
        }
    }

    for (unsigned int i = 1; i < srvcnt; ++i)
    {
        unsigned int a;
        while (queues[i]->Get(&a, indet->GetMainThread()))
            results[i].push_back(a);
    }

    for (unsigned int i = 0; i < srvcnt; ++i)
    {
        nodes[i]->Fini(indet->GetMainThread());
        delete nodes[i];
        delete queues[i];
        delete queue_locks[i];
        delete nets[i];
        delete cbs[i];
        delete rands[i];
        timers[i]->Fini(indet->GetMainThread());
        delete timers[i];
    }

    for (unsigned int i = 0; i < srvcnt; ++i)
    {
        std::string dmp;
        for (std::vector<unsigned int>::iterator it = results[i].begin();
                it != results[i].end();
                ++it)
        {
            if (dmp.size()) dmp += ", ";
            dmp += ToStr(*it);
        }
        INFO(logger, indet->GetMainThread(), "final applied results: %s\n", dmp.c_str());
        if (i != 0)
        {
            ASSERT(logger, indet->GetMainThread(), results[0].size() >= results[i].size());
            ASSERT(logger, indet->GetMainThread(),
                    std::vector<unsigned int>(results[0].begin(), results[0].begin() + results[i].size()) == results[i]);
        }
    }

    INFO(logger, indet->GetMainThread(), "done\n");

    delete changes;
    delete logger;
    delete logger_lock;
    delete indet;

    return 0;
}
