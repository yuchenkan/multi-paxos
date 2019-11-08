#include <assert.h>
#include <pthread.h>

#include <list>
#include <deque>
#include <vector>
#include <string>

std::string DumpHex(const char * buf, unsigned int len);

inline unsigned int AtomicGet(unsigned int * i) { return *i; }

inline bool AtomicGet(bool * b) { return *b; }

inline void AtomicSet(bool * b, bool v) { *b = v; }

inline unsigned int AtomicInc(unsigned int * i)
{
    return __sync_fetch_and_add(i, 1) + 1;
}

inline unsigned int AtomicDec(unsigned int * i)
{
    return __sync_fetch_and_sub(i, 1) - 1;
}

class SpinLock
{
public:
    SpinLock() { assert(pthread_spin_init(&lock_, 0) == 0); }
    ~SpinLock() { assert(pthread_spin_destroy(&lock_) == 0); }

    void Lock() { assert(pthread_spin_lock(&lock_) == 0); }

    void Unlock() { assert(pthread_spin_unlock(&lock_) == 0); }

private:
    pthread_spinlock_t lock_;
};

void SetThreadName(const std::string & name);
void UnsetThreadName();

template<typename T>
class Queue
{
public:
    void Put(const T & msg)
    {
        lock_.Lock();
        queue_.push_back(msg);
        lock_.Unlock();
    }

    bool Get(T * msg)
    {
        lock_.Lock();
        bool ret = !queue_.empty();
        if (!queue_.empty())
        {
            *msg = queue_.front();
            queue_.pop_front();
        }
        lock_.Unlock();
        return ret;
    }

    bool Empty()
    {
        lock_.Lock();
        bool ret = queue_.empty();
        lock_.Unlock();
        return ret;
    }

private:
    std::deque<T> queue_;
    SpinLock lock_;
};

typedef unsigned long long TimeStamp;

class Clock
{
public:
    virtual ~Clock() { }
    virtual TimeStamp Now() = 0;
};

class Logger
{
public:
    Logger(Clock * clock, unsigned int level): clock_(clock), level_(level) { }
    void Log(unsigned int level, const char * file, int line, const char * function, const char * format, ...);

private:
    Clock * clock_;
    unsigned int level_;
    SpinLock lock_;
};

#define TRACE(logger, fmt, args...) (logger)->Log(0, __FILE__, __LINE__, __FUNCTION__, fmt, ## args)
#define DEBUG(logger, fmt, args...) (logger)->Log(1, __FILE__, __LINE__, __FUNCTION__, fmt, ## args)
#define INFO(logger, fmt, args...) (logger)->Log(2, __FILE__, __LINE__, __FUNCTION__, fmt, ## args)
#define NOTICE(logger, fmt, args...) (logger)->Log(3, __FILE__, __LINE__, __FUNCTION__, fmt, ## args)
#define WARNING(logger, fmt, args...) (logger)->Log(4, __FILE__, __LINE__, __FUNCTION__, fmt, ## args)
#define ERROR(logger, fmt, args...) (logger)->Log(5, __FILE__, __LINE__, __FUNCTION__, fmt, ## args)
#define CRITICAL(logger, fmt, args...) (logger)->Log(6, __FILE__, __LINE__, __FUNCTION__, fmt, ## args)

#define ASSERT(logger, exp) do { if (!(exp)) { CRITICAL(logger, "assertion failed: " # exp "\n"); *(char *)0 = '\0'; } } while (0)

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
    ~Timer()
    {
        for (std::map<TimeStamp, std::list<Timeout *> >::iterator it = timeouts_.begin();
                it != timeouts_.end();
                ++it)
            for (std::list<Timeout *>::iterator jt = it->second.begin();
                    jt != it->second.end();
                    ++jt)
            {
                ASSERT(logger_, (*jt)->Canceled());
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

class Rand
{
public:
    Rand(int seed) : next_((unsigned long long)seed) { }

    unsigned long long Randomize(unsigned long long min, unsigned long long max)
    {
        next_ = next_ * 1103515245 + 12345;
        return min + next_ % (max - min);
    }

private:
    unsigned long long next_;
};

namespace paxos
{

class Paxos;
class PaxosImpl;

class NetWork
{
public:
    NetWork() : paxos_(NULL) { }

    virtual ~NetWork() { }
    virtual void SendMessageTCP(const std::string & ip, unsigned short port, const std::string & msg) = 0;
    virtual void SendMessageUDP(const std::string & ip, unsigned short port, const std::string & msg) = 0;

    void OnReceiveMessage(const char * msg, unsigned int len);

private:
    friend class PaxosImpl;

    void Init(PaxosImpl * paxos) { paxos_ = paxos; }
    void Fini() { }

private:
    PaxosImpl * paxos_;
};

class StateMachine
{
public:
    virtual ~StateMachine() { }

    virtual void Execute(const std::string & value) = 0;

    virtual std::string Debug(const std::string & value) const { return value; }
};

struct NodeInfo
{
    NodeInfo() { }
    NodeInfo(const std::string & ip, unsigned short port)
        : ip_(ip), port_(port) { }

    bool operator==(const NodeInfo & n)
    {
        return ip_ == n.ip_ && port_ == n.port_;
    }

    std::string ip_;
    unsigned short port_;
};

typedef std::map<unsigned int, NodeInfo> NodeInfoMap;

class Callback
{
public:
    virtual ~Callback() { }
    virtual void Run() = 0;
};

class Paxos
{
public:
    struct Config
    {
        Config()
            : prepare_delay_min_(1000), prepare_delay_max_(2000),
                prepare_retry_count_(3), prepare_retry_timeout_(500),
                accept_retry_count_(3), accept_retry_timeout_(500),
                commit_retry_timeout_(500) { }
        Config(const Config & c)
            : prepare_delay_min_(c.prepare_delay_min_),
                prepare_delay_max_(c.prepare_delay_max_),
                prepare_retry_count_(c.prepare_retry_count_),
                prepare_retry_timeout_(c.prepare_retry_timeout_),
                accept_retry_count_(c.accept_retry_count_),
                accept_retry_timeout_(c.accept_retry_timeout_),
                commit_retry_timeout_(c.commit_retry_timeout_) { }

        TimeStamp prepare_delay_min_;
        TimeStamp prepare_delay_max_;
        unsigned int prepare_retry_count_;
        TimeStamp prepare_retry_timeout_;
        unsigned int accept_retry_count_;
        TimeStamp accept_retry_timeout_;
        TimeStamp commit_retry_timeout_;
    };

public:
    // Passing timer in to support network delay from outside as
    // the delayed send must still be processed in paxos thread.
    // The timer is taken over by paxos and shall not processed
    // by others until the paxos is freed.
    Paxos(Logger * logger, const std::string & thread_prefix,
            Clock * clock, Timer * timer, Rand * rand,
            const NodeInfoMap & nodes, unsigned int index,
            NetWork * net, StateMachine * sm,
            const Config & config,
            unsigned int * whole_system_reference_count_for_debugging);
    ~Paxos();

    void Propose(const std::string & value, Callback * cb);

#if 0
    void AddMember(unsigned int id, const NodeInfo & node, Callback * cb);
    void DelMember(unsigned int id, Callback * cb);
#endif

private:
    PaxosImpl * impl_;
};

}
