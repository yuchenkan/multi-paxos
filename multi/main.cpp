#include <unistd.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/syscall.h>

#include <deque>
#include <string>
#include <set>
#include <map>
#include <sstream>
#include <algorithm>

#include "paxos.h"

struct Msg
{
    Msg() : sender_(NULL), sidx_((unsigned int)-1) { }
    Msg(Queue<Msg> * sender, unsigned int sidx, const std::string & msg) : sender_(sender), sidx_(sidx), msg_(msg) { }
    Msg(const Msg & msg) : sender_(msg.sender_), sidx_(msg.sidx_), msg_(msg.msg_) { }

    Queue<Msg> * sender_;
    unsigned int sidx_;
    std::string msg_;
};

std::string id2str(unsigned int id)
{
    std::ostringstream oss;
    oss << id;
    return oss.str();
}

unsigned int str2id(const std::string & str)
{
    unsigned int id;
    std::istringstream iss(str);
    iss >> id;
    return id;
}

struct ServerThreadElt
{
    Queue<Msg> server_;
    Queue<Msg> client_;
};

class THNetWork : public paxos::NetWork
{
public:
    struct HijackConfig
    {
        HijackConfig() : drop_rate_(0), dup_rate_(0), min_delay_(0), max_delay_(0) { }
        HijackConfig(const HijackConfig & c)
            : drop_rate_(c.drop_rate_), dup_rate_(c.dup_rate_),
                min_delay_(c.min_delay_), max_delay_(c.max_delay_) { }

        unsigned int drop_rate_; // 1 / 10000
        unsigned int dup_rate_;

        TimeStamp min_delay_;
        TimeStamp max_delay_;
    };

private:
    class SendDelay : public Timeout
    {
    public:
        SendDelay(THNetWork * net, const std::string & ip, unsigned short port, const std::string & msg)
            : net_(net), ip_(ip), port_(port), msg_(msg) { }
        virtual ~SendDelay() { }

        virtual void Process()
        {
            if (!canceled_)
                net_->DelayedSend(this, ip_, port_, msg_);
            delete this;
        }

    private:
        THNetWork * net_;
        std::string ip_;
        unsigned short port_;
        std::string msg_;
    };

public:
    THNetWork(Logger * logger, ServerThreadElt * srves, unsigned int me,
                Clock * clock, Timer * timer, Rand * rand, const HijackConfig & config)
        : logger_(logger), srves_(srves), me_(me),
            clock_(clock), timer_(timer), rand_(rand), config_(config) { }
    virtual ~THNetWork()
    {
        for (std::set<SendDelay *>::iterator it = delay_.begin();
                it != delay_.end();
                ++it)
            (*it)->Cancel();
    }

private:
    void Send(const std::string & ip, unsigned short port, const std::string & msg)
    {
        Msg smsg(&srves_[me_].server_, me_, msg);
        srves_[port].server_.Put(smsg);
    }

    void DelayedSend(SendDelay * delay, const std::string & ip, unsigned short port, const std::string & msg)
    {
        delay_.erase(delay);
        Send(ip, port, msg);
    }

    void HijackSend(const std::string & ip, unsigned short port, const std::string & msg, unsigned int dup)
    {
        if (!dup && config_.drop_rate_ && rand_->Randomize(0, 10000) < config_.drop_rate_) return;

        if (dup < 3 && config_.dup_rate_ && rand_->Randomize(0, 10000) < config_.dup_rate_)
            HijackSend(ip, port, msg, ++dup);

        if (config_.max_delay_)
        {
            SendDelay * delay = new SendDelay(this, ip, port, msg);
            ASSERT(logger_, delay_.insert(delay).second);
            timer_->AddTimeout(delay,
                                clock_->Now() + rand_->Randomize(config_.min_delay_, config_.max_delay_));
        }
        else
            Send(ip, port, msg);
    }

public:
    void SendMessageTCP(const std::string & ip, unsigned short port, const std::string & msg)
    {
        std::string dmp = DumpHex(msg.c_str(), msg.length());
        TRACE(logger_, "srv[%u] send to srv[%d] by tcp: %s\n", me_, port, dmp.c_str());

        HijackSend(ip, port, msg, 0);
    }

    void SendMessageUDP(const std::string & ip, unsigned short port, const std::string & msg)
    {
        std::string dmp = DumpHex(msg.c_str(), msg.length());
        TRACE(logger_, "srv[%u] send to srv[%d] by udp: %s\n", me_, port, dmp.c_str());

        HijackSend(ip, port, msg, 0);
    }

private:
    Logger * logger_;
    ServerThreadElt * srves_;
    unsigned int me_;

    Clock * clock_;
    Timer * timer_;
    Rand * rand_;
    HijackConfig config_;

    std::set<SendDelay *> delay_;
};

struct ServerThreadData
{
    Logger * logger_;
    Clock * clock_;
    int seed_;
    const paxos::Paxos::Config * paxos_config_;
    const THNetWork::HijackConfig * network_hijack_;

    unsigned int cltcnt_;
    ServerThreadElt * srves_;
    unsigned int srvcnt_;
    unsigned int index_;
    unsigned int idcnt_;
    unsigned int * total_;
    unsigned int * system_;
    std::vector<unsigned int> executed_ids_;
    pthread_t thread_;
};

struct ClientServerMessage
{
    unsigned int id_;
};

class SM : public paxos::StateMachine
{
public:
    SM(Logger * logger, unsigned int cltcnt, unsigned int idcnt,
        unsigned int * total, std::vector<unsigned int> * executed_ids)
        : logger_(logger), idcnt_(idcnt),
            total_(total), executed_ids_(executed_ids)
    {
        for (unsigned int i = 0; i < cltcnt / 2; ++i)
            ordered_ids_.insert(std::make_pair(i, i * idcnt));
    }

    virtual ~SM() { }

    virtual void Execute(const std::string & value)
    {
        unsigned int id = str2id(value);
        if (id / idcnt_ < ordered_ids_.size() && id % idcnt_ <= idcnt_ / 2)
        {
            ASSERT(logger_, ordered_ids_[id / idcnt_] == id);
            ++ordered_ids_[id / idcnt_];
        }
        AtomicInc(total_);
        executed_ids_->push_back(id);
    }

    virtual std::string Debug(const std::string & value) const
    {
        char buf[32];
        snprintf(buf, 32, "%u", str2id(value));
        return std::string(buf);
    }

private:
    Logger * logger_;
    unsigned int idcnt_;
    std::map<unsigned int, unsigned int> ordered_ids_;
    unsigned int * total_;
    std::vector<unsigned int> * executed_ids_;
};

struct ReplyData
{
public:
    ReplyData(Queue<Msg> * sender, unsigned int sidx, Queue<Msg> * receiver)
        : sender_(sender), sidx_(sidx), receiver_(receiver) { }
    ReplyData(const ReplyData & reply)
        : sender_(reply.sender_), sidx_(reply.sidx_), receiver_(reply.receiver_) { }

    Queue<Msg> * sender_;
    unsigned int sidx_;

    Queue<Msg> * receiver_;
};

class RealTimeClock : public Clock
{
public:
    virtual ~RealTimeClock() { }
    virtual TimeStamp Now()
    {
        timespec spec;
        clock_gettime(CLOCK_REALTIME, &spec);
        return (TimeStamp)(spec.tv_sec * 1000 + spec.tv_nsec / 1.0e6);
    }
};

struct Consensus
{
    Consensus(ServerThreadData * srv, Clock * clock, Timer * timer, Rand * rand)
        : logger_(srv->logger_),
            sm_(logger_, srv->cltcnt_, srv->idcnt_,
                srv->total_, &srv->executed_ids_),
            net_(logger_, srv->srves_, srv->index_,
                    clock, timer, rand, *srv->network_hijack_),
            paxos_(NULL)
    {
        std::string ip = "0.0.0.0";
        paxos::NodeInfoMap nodes;
        for (unsigned int i = 0; i < srv->srvcnt_; ++i)
            nodes.insert(std::make_pair(i, paxos::NodeInfo(ip, (unsigned short)i)));

        char prefix[64];
        snprintf(prefix, 64, "srv-%u", srv->index_);
        paxos_ = new paxos::Paxos(logger_, prefix, srv->clock_,
                                    timer, rand, nodes, srv->index_, &net_, &sm_,
                                    *srv->paxos_config_,
                                    srv->system_);
    }

    ~Consensus() { delete paxos_; }

    struct ProposeCallback : public paxos::Callback
    {
        ProposeCallback(unsigned int id, const ReplyData * reply)
            : id_(id), reply_(*reply) { }
        virtual ~ProposeCallback() { }

        virtual void Run()
        {
            Msg reply(reply_.sender_, reply_.sidx_, id2str(id_));
            reply_.receiver_->Put(reply);

            delete this;
        }

        unsigned int id_;
        ReplyData reply_;
    };

    void Propose(unsigned int id, const ReplyData  * reply)
    {
        ProposeCallback * cb = new ProposeCallback(id, reply);
        paxos_->Propose(id2str(id), cb);
    }

    Logger * logger_;
    SM sm_;
    THNetWork net_;

    paxos::Paxos * paxos_;
};

void * StartServer(void * data)
{
    ServerThreadData * srv = (ServerThreadData *)data;
    ServerThreadElt * elt = srv->srves_ + srv->index_;

    RealTimeClock clock;
    Timer timer(srv->logger_);
    Rand rand(srv->seed_ + srv->srvcnt_);

    char thread[64];
    snprintf(thread, 64, "srv-%u", srv->index_);
    SetThreadName(thread);

    DEBUG(srv->logger_, "start srv[%u]\n", srv->index_);

    Consensus * cons = new Consensus(srv, &clock, &timer, &rand);

    DEBUG(srv->logger_, "srv[%u] enter loop\n", srv->index_);
    while (AtomicGet(srv->total_) != srv->srvcnt_ * srv->cltcnt_ * srv->idcnt_ || AtomicGet(srv->system_))
    {
        bool busy = false;

        Msg msg;
        if (elt->client_.Get(&msg))
        {
            busy = true;

            ClientServerMessage * csmsg = (ClientServerMessage *)msg.msg_.c_str();
            DEBUG(srv->logger_, "srv[%u] receive from clt[%u]: id %d\n",
                    srv->index_, msg.sidx_, csmsg->id_);

            ReplyData reply(&elt->client_, srv->index_, msg.sender_);
            cons->Propose(csmsg->id_, &reply);
        }

        if (elt->server_.Get(&msg))
        {
            busy = true;

            std::string dmp = DumpHex(msg.msg_.c_str(), msg.msg_.length());
            TRACE(srv->logger_, "srv[%u] receive from srv[%u]: %s\n",
                    srv->index_, msg.sidx_, dmp.c_str());
            cons->net_.OnReceiveMessage(msg.msg_.c_str(), msg.msg_.length());
        }

        if (!busy)
            usleep(100);
    }

    DEBUG(srv->logger_, "srv[%u] exit loop\n", srv->index_);

    delete cons;

    UnsetThreadName();

    return NULL;
}

struct ClientThreadData
{
    Logger * logger_;
    unsigned int index_;

    unsigned int idcnt_;
    unsigned int propose_interval_;
    Queue<Msg> server_;
    ServerThreadElt * srves_;
    unsigned int srvcnt_;
    unsigned int cltcnt_;
    pthread_t thread_;
};

void * StartClient(void * data)
{
    ClientThreadData * clt = (ClientThreadData *)data;

    std::map<unsigned int, unsigned int> ids;
    unsigned int start = clt->index_ * clt->idcnt_;
    unsigned int end = start + clt->idcnt_;

    char thread[64];
    snprintf(thread, 64, "clt-%u", clt->index_);
    SetThreadName(thread);

    usleep(1000 * clt->propose_interval_ * clt->index_);
    DEBUG(clt->logger_, "start clt[%u]\n", clt->index_);

    // Half ids from half clients are proposed in order,
    // i.e. next id is proposed only after current id is
    // committed
    bool inorder = (clt->index_ < clt->cltcnt_ / 2);

    // TODO
    //bool fixed = (clt->srvcnt_ + 1) / 2;
    //bool add = clt->srvcnt_ - fixed;
    //bool del = (add + 1) / 2;

    unsigned int current = start;
    while (true)
    {
        if (current != end && (!inorder || (current - start) > clt->idcnt_ / 2 || ids.empty()))
        //if (current != end && ids.empty())
        {
            unsigned int sidx = clt->srvcnt_ - 1 - (current - start) % clt->srvcnt_;
            //unsigned int sidx = 0;
            DEBUG(clt->logger_, "send id %d to %d\n", current, sidx);

            Queue<Msg> * server = &clt->srves_[sidx].client_;

            ClientServerMessage csmsg;
            csmsg.id_ = current;
            Msg msg(&clt->server_, clt->index_, std::string((const char *)&csmsg, sizeof csmsg));
            server->Put(msg);

            ASSERT(clt->logger_, ids.insert(std::make_pair(current, sidx)).second);
            ++current;
        }

        Msg msg;
        if (clt->server_.Get(&msg))
        {
            unsigned int id = str2id(msg.msg_);
            if (ids.find(id) == ids.end())
                ERROR(clt->logger_, "id %u not found\n", id);
            else if (msg.sidx_ != ids[id])
                ERROR(clt->logger_, "expect id %u received from %u, got %u\n", id, ids[id], msg.sidx_);
            ASSERT(clt->logger_, ids.find(id) != ids.end() && msg.sidx_ == ids[id]);
            ids.erase(id);

            DEBUG(clt->logger_, "receive id %d from %u\n", id, msg.sidx_);
        }

        if (current == end && ids.empty())
            break;

        usleep(1000 * clt->propose_interval_ * clt->cltcnt_);
    }

    DEBUG(clt->logger_, "clt[%u] exit loop\n", clt->index_);

    UnsetThreadName();

    return NULL;
}

int main(int argc, char ** argv)
{
    unsigned int args[4];
    unsigned int argn = 0;

    unsigned int level = 0;
    int seed = (int)time(NULL);

    paxos::Paxos::Config paxos_config;
    THNetWork::HijackConfig network_hijack;

    for (int i = 1; i < argc; ++i)
        if (strncmp(argv[i], "--log-level=", strlen("--log-level=")) == 0)
            level = (unsigned int)atoi(argv[i] + strlen("--log-level="));
        else if (strncmp(argv[i], "--seed=", strlen("--seed=")) == 0)
            seed = (int)atoi(argv[i] + strlen("--seed="));
        else if (strncmp(argv[i], "--paxos-prepare-delay-min=", strlen("--paxos-prepare-delay-min=")) == 0)
            paxos_config.prepare_delay_min_ = (TimeStamp)atoi(argv[i] + strlen("--paxos-prepare-delay-min="));
        else if (strncmp(argv[i], "--paxos-prepare-delay-max=", strlen("--paxos-prepare-delay-max=")) == 0)
            paxos_config.prepare_delay_max_ = (TimeStamp)atoi(argv[i] + strlen("--paxos-prepare-delay-max="));
        else if (strncmp(argv[i], "--paxos-prepare-retry-count=", strlen("--paxos-prepare-retry-count=")) == 0)
            paxos_config.prepare_retry_count_ = (unsigned int)atoi(argv[i] + strlen("--paxos-prepare-retry-count="));
        else if (strncmp(argv[i], "--paxos-prepare-retry-timeout=", strlen("--paxos-prepare-retry-timeout=")) == 0)
            paxos_config.prepare_retry_timeout_ = (TimeStamp)atoi(argv[i] + strlen("--paxos-prepare-retry-timeout="));
        else if (strncmp(argv[i], "--paxos-accept-retry-count=", strlen("--paxos-accept-retry-count=")) == 0)
            paxos_config.accept_retry_count_ = (unsigned int)atoi(argv[i] + strlen("--paxos-accept-retry-count="));
        else if (strncmp(argv[i], "--paxos-accept-retry-timeout=", strlen("--paxos-accept-retry-timeout=")) == 0)
            paxos_config.accept_retry_timeout_ = (TimeStamp)atoi(argv[i] + strlen("--paxos-accept-retry-timeout="));
        else if (strncmp(argv[i], "--paxos-commit-retry-timeout=", strlen("--paxos-commit-retry-timeout=")) == 0)
            paxos_config.commit_retry_timeout_ = (TimeStamp)atoi(argv[i] + strlen("--paxos-commit-retry-timeout="));
        else if (strncmp(argv[i], "--net-drop-rate=", strlen("--net-drop-rate=")) == 0)
            network_hijack.drop_rate_ = (unsigned int)atoi(argv[i] + strlen("--net-drop-rate="));
        else if (strncmp(argv[i], "--net-dup-rate=", strlen("--net-dup-rate=")) == 0)
            network_hijack.dup_rate_ = (unsigned int)atoi(argv[i] + strlen("--net-dup-rate="));
        else if (strncmp(argv[i], "--net-min-delay=", strlen("--net-min-delay=")) == 0)
            network_hijack.min_delay_ = (TimeStamp)atoi(argv[i] + strlen("--net-min-delay="));
        else if (strncmp(argv[i], "--net-max-delay=", strlen("--net-max-delay=")) == 0)
            network_hijack.max_delay_ = (TimeStamp)atoi(argv[i] + strlen("--net-max-delay="));
        else
            args[argn++] = (unsigned int)atoi(argv[i]);
    assert(argn == 4);

    unsigned int srvcnt = args[0];
    unsigned int cltcnt = args[1];
    unsigned int idcnt = args[2];
    unsigned int propose_interval = args[3];

    RealTimeClock clock;
    Logger logger(&clock, level);

    ASSERT(&logger, srvcnt != 0);
    ASSERT(&logger, cltcnt != 0);

    INFO(&logger, "seed = %u\n", seed);
    INFO(&logger,
            "paxos prepare delay min = %llu ms, prepare delay max = %llu ms, "
            "prepare retry count = %u, prepare retry timeout = %llu ms, "
            "accept retry count = %u, accept retry timeout = %llu ms, "
            "commit retry timeout = %llu\n",
            paxos_config.prepare_delay_min_, paxos_config.prepare_delay_max_,
            paxos_config.prepare_retry_count_, paxos_config.prepare_retry_timeout_,
            paxos_config.accept_retry_count_, paxos_config.accept_retry_timeout_,
            paxos_config.commit_retry_timeout_);
    INFO(&logger, "network drop rate = %u / 10000, dup rate = %u / 10000, min delay = %llu ms, max delay = %llu ms\n",
            network_hijack.drop_rate_, network_hijack.dup_rate_,
            network_hijack.min_delay_, network_hijack.max_delay_);

    unsigned int total = 0;
    unsigned int system = 0;

    ServerThreadElt * srves = new ServerThreadElt[srvcnt];
    ServerThreadData * srvs = new ServerThreadData[srvcnt];
    for (unsigned int i = 0; i < srvcnt; ++i)
    {
        srvs[i].logger_ = &logger;
        srvs[i].clock_ = &clock;
        srvs[i].seed_ = seed + i;
        srvs[i].paxos_config_ = &paxos_config;
        srvs[i].network_hijack_ = &network_hijack;

        srvs[i].cltcnt_ = cltcnt;
        srvs[i].srves_ = srves;
        srvs[i].srvcnt_ = srvcnt;
        srvs[i].index_ = i;
        srvs[i].idcnt_ = idcnt;
        srvs[i].total_ = &total;
        srvs[i].system_ = &system;

        ASSERT(&logger, pthread_create(&srvs[i].thread_, NULL, StartServer, srvs + i) == 0);
    }

    ClientThreadData * clts = new ClientThreadData[cltcnt];
    for (unsigned int i = 0; i < cltcnt; ++i)
    {
        clts[i].logger_ = &logger;
        clts[i].index_ = i;
        clts[i].idcnt_ = idcnt;
        clts[i].propose_interval_ = propose_interval;

        clts[i].srves_ = srves;
        clts[i].srvcnt_ = srvcnt;
        clts[i].cltcnt_ = cltcnt;
        ASSERT(&logger, pthread_create(&clts[i].thread_, NULL, StartClient, clts + i) == 0);
    }

    for (unsigned int i = 0; i < cltcnt; ++i)
        ASSERT(&logger, pthread_join(clts[i].thread_, NULL) == 0);

    for (unsigned int i = 0; i < srvcnt; ++i)
        ASSERT(&logger, pthread_join(srvs[i].thread_, NULL) == 0);

    ASSERT(&logger, srvs[0].executed_ids_.size() == cltcnt * idcnt);
    for (unsigned int i = 1; i < srvcnt; ++i)
        ASSERT(&logger, srvs[0].executed_ids_ == srvs[i].executed_ids_);
    std::vector<unsigned int> sorted_executed_ids = srvs[0].executed_ids_;
    sort(sorted_executed_ids.begin(), sorted_executed_ids.end());
    for (unsigned int i = 0; i < cltcnt * idcnt; ++i)
        ASSERT(&logger, sorted_executed_ids[i] == i);

    delete [] clts;
    delete [] srvs;
    delete [] srves;

    INFO(&logger, "All done\n");
    return 0;
}
