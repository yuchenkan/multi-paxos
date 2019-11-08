#include <time.h>
#include <stdlib.h>
#include <unistd.h>

#include "indet.h"

std::string GetStr(FILE * fp)
{
    char * buf = NULL;
    size_t n = 0;
    ssize_t r = getline(&buf, &n, fp);
    std::string ret;
    if (r < 0)
        assert(feof(fp));
    else
    {
        assert(r > 0 && buf[r - 1] == '\n');
        ret = std::string(buf, r - 1);
    }
    free(buf);
    return ret;
}

class RealClock : public Clock
{
public:
    virtual ~RealClock() { }

    virtual TimeStamp Now(const Thread * thread)
    {
        timespec spec;
        clock_gettime(CLOCK_REALTIME, &spec);
        TimeStamp timestamp = (TimeStamp)(spec.tv_sec * 1000 + spec.tv_nsec / 1.0e6);

        assert(fprintf(thread->GetClockLog(), "%llu\n", timestamp) > 0);
        assert(fflush(thread->GetClockLog()) == 0);
        return timestamp;
    }
};

class ReplayClock : public Clock
{
public:
    virtual ~ReplayClock() { }

    virtual TimeStamp Now(const Thread * thread)
    {
        TimeStamp timestamp;
        if (fscanf(thread->GetClockLog(), "%llu", &timestamp) != 1)
            while (true) continue;
        return timestamp;
    }
};

class SimpleSpinLock
{
public:
    SimpleSpinLock() { assert(pthread_spin_init(&lock_, 0) == 0); }
    ~SimpleSpinLock() { assert(pthread_spin_destroy(&lock_) == 0); }

    void Lock() { assert(pthread_spin_lock(&lock_) == 0); }

    void Unlock() { assert(pthread_spin_unlock(&lock_) == 0); }

private:
    pthread_spinlock_t lock_;
};

class RealSpinLock : public SpinLock
{
public:
    RealSpinLock(const std::string & log) : fp_(NULL)
    {
        assert(fp_ = fopen(log.c_str(), "w"));
    }
    virtual ~RealSpinLock()
    {
        assert(fclose(fp_) == 0);
    }

    virtual void Lock(const Thread * thread) 
    {
        lock_.Lock();
        assert(fprintf(fp_, "%s\n", thread->GetID().c_str()) > 0);
        assert(fflush(fp_) == 0);
    }

    virtual void Unlock()
    {
        lock_.Unlock();
    }

private:
    FILE * fp_;
    SimpleSpinLock lock_;
};

class ReplaySpinLock : public SpinLock
{
public:
    ReplaySpinLock(const std::string & log) : fp_(NULL)
    {
        assert(fp_ = fopen(log.c_str(), "r"));
        next_ = GetStr(fp_);
    }
    virtual ~ReplaySpinLock()
    {
        assert(fclose(fp_) == 0);
    }

    virtual void Lock(const Thread * thread)
    {
        //assert(!GetNext().empty());
        while (GetNext() != thread->GetID())
            continue;

        lock_.Lock();
        SetNext(GetStr(fp_));
    }

    virtual void Unlock()
    {
        lock_.Unlock();
    }

private:
    std::string GetNext()
    {
        std::string next;
        next_lock_.Lock();
        next = next_;
        next_lock_.Unlock();
        return next;
    }

    void SetNext(const std::string & next)
    {
        next_lock_.Lock();
        next_ = next;
        next_lock_.Unlock();
    }

private:
    FILE * fp_;
    std::string next_;
    SimpleSpinLock next_lock_;
    SimpleSpinLock lock_;
};

class RealAtomicBool : public AtomicBool
{
public:
    RealAtomicBool(bool b, const std::string & log)
        : AtomicBool(b), lock_(log) { }
    virtual ~RealAtomicBool() { }

    virtual void Set(bool b, const Thread * thread)
    {
        lock_.Lock(thread);
        b_ = b;
        lock_.Unlock();
    }

    virtual bool Get(const Thread * thread)
    {
        bool b;
        lock_.Lock(thread);
        b = b_;
        lock_.Unlock();
        return b;
    }

private:
    RealSpinLock lock_;
};

class ReplayAtomicBool : public AtomicBool
{
public:
    ReplayAtomicBool(bool b, const std::string & log)
        : AtomicBool(b), lock_(log) { }
    virtual ~ReplayAtomicBool() { }

    virtual void Set(bool b, const Thread * thread)
    {
        lock_.Lock(thread);
        b_ = b;
        lock_.Unlock();
    }

    virtual bool Get(const Thread * thread)
    {
        bool b;
        lock_.Lock(thread);
        b = b_;
        lock_.Unlock();
        return b;
    }

private:
    ReplaySpinLock lock_;
};

class RealThread : public Thread
{
public:
    RealThread(const std::vector<ThreadID> & thread_id,
                const std::string & name, const std::string & dir,
                const Rand & rand, unsigned long long failure_rate)
        : Thread(thread_id, name, dir, rand, failure_rate) { }
    virtual ~RealThread() { }

    virtual void USleep(unsigned long long time) { usleep(time); }
    virtual void Sleep(unsigned long long time) { sleep(time); }

private:
    virtual SpinLock * NewSpinLock(const std::string & log)
    {
        return new RealSpinLock(log);
    }

    virtual AtomicBool * NewAtomicBool(bool b, const std::string & log)
    {
        return new RealAtomicBool(b, log);
    }

    virtual Thread * NewThread(const std::vector<ThreadID> & thread_id,
                                const std::string & name, const std::string & dir,
                                const Rand & rand, unsigned long long failure_rate)
    {
        return new RealThread(thread_id, name, dir, rand, failure_rate);
    }

    virtual FILE * OpenClockLog(const std::string & log)
    {
        FILE * fp;
        assert(fp = fopen(log.c_str(), "w"));
        return fp;
    }
};

class ReplayThread : public Thread
{
public:
    ReplayThread(const std::vector<ThreadID> & thread_id,
                    const std::string & name, const std::string & dir,
                    const Rand & rand, unsigned long long failure_rate)
        : Thread(thread_id, name, dir, rand, failure_rate) { }
    virtual ~ReplayThread() { }

    virtual void USleep(unsigned long long time) { }
    virtual void Sleep(unsigned long long time) { }

private:
    virtual SpinLock * NewSpinLock(const std::string & log)
    {
        return new ReplaySpinLock(log);
    }

    virtual AtomicBool * NewAtomicBool(bool b, const std::string & log)
    {
        return new ReplayAtomicBool(b, log);
    }

    virtual Thread * NewThread(const std::vector<ThreadID> & thread_id,
                                const std::string & name, const std::string & dir,
                                const Rand & rand, unsigned long long failure_rate)
    {
        return new ReplayThread(thread_id, name, dir, rand, failure_rate);
    }

    virtual FILE * OpenClockLog(const std::string & log)
    {
        FILE * fp;
        assert(fp = fopen(log.c_str(), "r"));
        return fp;
    }
};

Indet::Indet(const std::string & main_thread_name, const std::string & dir, bool replay)
    : clock_(!replay ? (Clock *) new RealClock : new ReplayClock)
{
    std::vector<ThreadID> thread_id;
    thread_id.push_back(0);
    main_ = !replay
                ? (Thread *) new RealThread(thread_id, main_thread_name, dir,
                                            Rand(0), 0)
                : new ReplayThread(thread_id, main_thread_name, dir,
                                    Rand(0), 0);
    main_->OpenClockLog();
}

Indet::~Indet()
{
    delete main_;
    delete clock_;
}
