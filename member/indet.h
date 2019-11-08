#ifndef INDET_H
#define INDET_H

#include <assert.h>
#include <pthread.h>
#include <stdio.h>

#include <sstream>
#include <vector>
#include <string>

template <typename T>
static std::string ToStr(const T & v)
{
    std::ostringstream oss;
    oss << v;
    return oss.str();
}

template <typename T>
static T FromStr(const std::string & s)
{
    T v;
    std::istringstream(s) >> v;
    return v;
}

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

typedef unsigned long long TimeStamp;

class Thread;

class Clock
{
public:
    virtual ~Clock() { }
    virtual TimeStamp Now(const Thread * thread) = 0;
};

class SpinLock
{
public:
    virtual ~SpinLock() { }
    virtual void Lock(const Thread * thread) = 0;
    virtual void Unlock() = 0;
};

class AtomicBool
{
public:
    AtomicBool(bool b) : b_(b) { }
    virtual ~AtomicBool() { }
    virtual void Set(bool b, const Thread * thread) = 0;
    virtual bool Get(const Thread * thread) = 0;

protected:
    bool b_;
};

typedef unsigned int ThreadID;

void a();

class Thread
{
public:
    Thread(const std::vector<ThreadID> & thread_id,
            const std::string & name, const std::string & dir,
            const Rand & rand, unsigned long long failure_rate)
        : main_(true), thread_id_(thread_id), child_id_(0),
            name_(name), dir_(dir),
            rand_(rand), failure_rate_(failure_rate),
            atomic_bool_id_(0), spin_lock_id_(0),
            clock_log_(NULL) { }
    virtual ~Thread()
    {
        if (clock_log_)
            assert(fclose(clock_log_) == 0);
    }

    void Join()
    {
        assert(!main_);
        assert(pthread_join(tid_, NULL) == 0);
    }

    SpinLock * NewSpinLock()
    {
        return NewSpinLock(dir_ + "/spin-lock-" + GetID() + '.' + ToStr(spin_lock_id_++) + ".log");
    }

    AtomicBool * NewAtomicBool(bool b)
    {
        return NewAtomicBool(b, dir_ + "/atomic-bool-" + GetID() + '.' + ToStr(atomic_bool_id_++) + ".log");
    }

    void NewThread(Thread ** thread, void * (* fn)(void *), void * arg, const std::string & name)
    {
        std::vector<ThreadID> thread_id = thread_id_;
        thread_id.push_back(child_id_++);
        *thread = NewThread(thread_id, name, dir_,
                            Rand((int)rand_.Randomize(0, (unsigned long long)-1)), failure_rate_);
        (*thread)->main_ = false;
        (*thread)->OpenClockLog();
        assert(pthread_create(&(*thread)->tid_, NULL, fn, arg) == 0);
    }

    const std::string GetID() const
    {
        std::string tid;
        for (std::vector<ThreadID>::const_iterator it = thread_id_.begin(); it != thread_id_.end(); ++it)
        {
            if (tid.size()) tid += '.';
            tid += ToStr(*it);
        }
        return tid;
    }
    const std::string & GetName() const { return name_; }

    void OpenClockLog()
    {
        clock_log_ = OpenClockLog(dir_ + "/clock-" + GetID() + ".log");
    }
    FILE * GetClockLog() const { return clock_log_; }

    void StartRandomFailure(int seed, unsigned long long failure_rate)
    {
        rand_ = Rand(seed);
        failure_rate_ = failure_rate;
    }

    void RandomFailure()
    {
        if (failure_rate_ && rand_.Randomize(0, 1000000) < failure_rate_)
            assert(false);
    }

    virtual void USleep(unsigned long long time) = 0;
    virtual void Sleep(unsigned long long time) = 0;

private:
    virtual SpinLock * NewSpinLock(const std::string & log) = 0;
    virtual AtomicBool * NewAtomicBool(bool b, const std::string & log) = 0;
    virtual Thread * NewThread(const std::vector<ThreadID> & thread_id,
                                const std::string & name, const std::string & dir,
                                const Rand & rand, unsigned long long failure_rate) = 0;
    virtual FILE * OpenClockLog(const std::string & log) = 0;

private:
    bool main_;
    pthread_t tid_;

    std::vector<ThreadID> thread_id_;
    ThreadID child_id_;

    std::string name_;
    std::string dir_;

    Rand rand_;
    unsigned long long failure_rate_;

    unsigned int atomic_bool_id_;
    unsigned int spin_lock_id_;

    FILE * clock_log_;
};

class Indet
{
public:
    Indet(const std::string & main_thread_name, const std::string & dir, bool replay);
    ~Indet();

    Clock * GetClock() { return clock_; }
    Thread * GetMainThread() { return main_; }

private:
    Clock * clock_;
    Thread * main_;
};

#endif
