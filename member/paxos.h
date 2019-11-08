#include <assert.h>
#include <string.h>

#include <map>
#include <list>
#include <deque>
#include <string>

#include "indet.h"

template<typename T>
class Queue
{
public:
    Queue(SpinLock * lock) : lock_(lock) { }

    void Put(const T & msg, const Thread * thread)
    {
        lock_->Lock(thread);
        queue_.push_back(msg);
        lock_->Unlock();
    }

    bool Get(T * msg, const Thread * thread)
    {
        lock_->Lock(thread);
        bool ret = !queue_.empty();
        if (!queue_.empty())
        {
            *msg = queue_.front();
            queue_.pop_front();
        }
        lock_->Unlock();
        return ret;
    }

#if 0
    bool Empty(const Thread * thread)
    {
        lock_->Lock(thread);
        bool ret = queue_.empty();
        lock_->Unlock();
        return ret;
    }
#endif

private:
    std::deque<T> queue_;
    SpinLock * lock_;
};

class Logger
{
public:
    Logger(SpinLock * lock, Clock * clock, unsigned int level)
        : lock_(lock), clock_(clock), level_(level) { }
    void Log(unsigned int level, Thread * thread,
                const char * file, int line, const char * function, const char * format, ...);

private:
    SpinLock * lock_;
    Clock * clock_;
    unsigned int level_;
};

#define TRACE(logger, thread, fmt, args...) (logger)->Log(0, thread, __FILE__, __LINE__, __FUNCTION__, fmt, ## args)
#define DEBUG(logger, thread, fmt, args...) (logger)->Log(1, thread, __FILE__, __LINE__, __FUNCTION__, fmt, ## args)
#define INFO(logger, thread, fmt, args...) (logger)->Log(2, thread, __FILE__, __LINE__, __FUNCTION__, fmt, ## args)
#define NOTICE(logger, thread, fmt, args...) (logger)->Log(3, thread, __FILE__, __LINE__, __FUNCTION__, fmt, ## args)
#define WARNING(logger, thread, fmt, args...) (logger)->Log(4, thread, __FILE__, __LINE__, __FUNCTION__, fmt, ## args)
#define ERROR(logger, thread, fmt, args...) (logger)->Log(5, thread, __FILE__, __LINE__, __FUNCTION__, fmt, ## args)
#define CRITICAL(logger, thread, fmt, args...) (logger)->Log(6, thread, __FILE__, __LINE__, __FUNCTION__, fmt, ## args)

#define ASSERT(logger, thread, exp) do { if (!(exp)) { CRITICAL(logger, thread, "assertion failed: " # exp "\n"); *(char *)0 = '\0'; } } while (0)

class Timeout
{
public:
    Timeout() : canceled_(false)  {}
    virtual ~Timeout() { }
    virtual void Process() = 0;

    void Cancel() { canceled_ = true; }
    bool Canceled() const { return canceled_; }

protected:
    bool canceled_;
};

class Timer
{
public:
    Timer(Logger * logger) : logger_(logger) { }

    void Fini(Thread * thread)
    {
        for (std::map<TimeStamp, std::list<Timeout *> >::iterator it = timeouts_.begin();
                it != timeouts_.end();
                ++it)
            for (std::list<Timeout *>::iterator jt = it->second.begin();
                    jt != it->second.end();
                    ++jt)
            {
                ASSERT(logger_, thread, (*jt)->Canceled());
                (*jt)->Process();
            }
    }

    void AddTimeout(Timeout * timeout, TimeStamp future)
    {
        timeouts_[future].push_back(timeout);
    }

    void Process(TimeStamp now)
    {
        while (!timeouts_.empty() && timeouts_.begin()->first <= now)
        {
            std::map<TimeStamp, std::list<Timeout *> >::iterator it = timeouts_.begin();
            while (!it->second.empty())
            {
                it->second.front()->Process();
                it->second.pop_front();
            }
            timeouts_.erase(it);
        }
    }

    bool Empty() const { return timeouts_.empty(); }

private:
    Logger * logger_;

    // TODO use a multimap
    std::map<TimeStamp, std::list<Timeout *> > timeouts_;
};

namespace paxos
{

typedef unsigned int NodeID;

class Callback
{
public:
    virtual ~Callback() { }

    // Called if the node is not a proposer
    virtual void Unproposable(Thread * thread, const std::string & cb) { }

    // Accepted is called after value is accepted by
    // majority acceptors. These values are safe as
    // long as majority acceptors are alive
    virtual void Accepted(Thread * thread, const std::string & cb) { }

    // Applied is called after value is applied in the
    // majority of (old/new) acceptors. It's used by adding acceptors
    // which requires next change(m_i+1) is proposed strictly
    // affter the current change(m_i) is applied by
    // the majority of old acceptors(m_i-1)
    // TODO the behavior when involved with accpetor changes for
    // normal values is not fully considered
    virtual void Applied(Thread * thread, const std::string & cb, const std::string * result) { }
};

class StateMachine
{
public:
    virtual ~StateMachine() { }
    virtual bool Apply(Thread * thread, const std::string & value, std::string * result) = 0;
};

struct NodeImpl;

class NetWork
{
public:
    NetWork(): node_(NULL) { }
    virtual ~NetWork() { }

    virtual void Send(Thread * thread, NodeID node, const std::string & msg) = 0;
    void OnReceive(const std::string & msg, const Thread * thread);

private:
    friend class NodeImpl;

    void Init(NodeImpl * node) { node_ = node; }
    void Fini() { node_ = NULL; }

private:
    NodeImpl * node_;
};

struct Config
{
    Config()
        : prepare_delay_min_(1000), prepare_delay_max_(2000),
            prepare_retry_count_(3), prepare_retry_timeout_(500),
            accept_retry_count_(3), accept_retry_timeout_(500),
            learn_retry_timeout_(500) { }
    Config(const Config & c)
        : prepare_delay_min_(c.prepare_delay_min_),
            prepare_delay_max_(c.prepare_delay_max_),
            prepare_retry_count_(c.prepare_retry_count_),
            prepare_retry_timeout_(c.prepare_retry_timeout_),
            accept_retry_count_(c.accept_retry_count_),
            accept_retry_timeout_(c.accept_retry_timeout_),
            learn_retry_timeout_(c.learn_retry_timeout_) { }

    TimeStamp prepare_delay_min_;
    TimeStamp prepare_delay_max_;
    unsigned int prepare_retry_count_;
    TimeStamp prepare_retry_timeout_;
    unsigned int accept_retry_count_;
    TimeStamp accept_retry_timeout_;
    TimeStamp learn_retry_timeout_;
};

class Paxos;

class Node
{
public:
    // Paxos is started with one node, which is also a
    // propser and acceptor, to have a minimized workable
    // system. New nodes can be added through this
    // node
    Node(Thread * thread, NodeID id, NodeID first,
            Logger * logger, Clock * clock, Timer * timer, Rand * rand,
            Callback * cb, NetWork * net, StateMachine * sm,
            const Config & config);
    ~Node();

    void Init(const std::string & thread_prefix, Thread * thread);
    void Fini(const Thread * thread);

    // Proposer
    void Propose(const std::string & value, const std::string & cb, const Thread * thread);

    // Membership change.
    // The hierarchy of inheritance is:
    // Learner : proposer : acceptor
    // Therefore, every node is a learner, and every
    // acceptor is also a proposer

    // Acceptor changes need to be done in an order
    // stricter than others. Next change(m_i+1)
    // requires to be proposed until the current
    // change(m_i) is applied by the majority of
    // old acceptors(m_i-1)
    void AddLearner(NodeID id, const std::string &cb, const Thread * thread);
    void AddProposer(NodeID id, const std::string & cb, const Thread * thread);
    void AddAcceptor(NodeID id, const std::string & cb, const Thread * thread);
    void LearnerToProposer(NodeID id, const std::string & cb, const Thread * thread);
    void LearnerToAcceptor(NodeID id, const std::string & cb, const Thread * thread);
    void ProposerToAcceptor(NodeID id, const std::string & cb, const Thread * thread);

    void DelLearner(NodeID id, const std::string & cb, const Thread * thread);
    void DelProposer(NodeID id, const std::string & cb, const Thread * thread);
    void DelAcceptor(NodeID id, const std::string & cb, const Thread * thread);
    void ProposerToLearner(NodeID id, const std::string & cb, const Thread * thread);
    void AcceptorToLearner(NodeID id, const std::string & cb, const Thread * thread);
    void AcceptorToProposer(NodeID id, const std::string & cb, const Thread * thread);

private:
    NodeImpl * impl_;
};

}
