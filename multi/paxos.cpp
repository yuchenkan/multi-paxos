#include <string.h>
#include <stdarg.h>
#include <time.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>

#include <set>
#include <map>
#include <algorithm>
#include <sstream>

#include "paxos.h"

// TODO check possible memory leak by canceling unadded prepare_retry_timeout_

// Debug value format:
//   no-op:      [instance-id] = <proposal-id>(proposer:value-id)-
//   normal:     [instance-id] = <proposal-id>(proposer:value-id)+value
//   add member: [instance-id] = <proposal-id>(proposer:value-id)m+id=ip:port
//   del member: [instance-id] = <proposal-id>(proposer:value-id)m-id

template <typename T>
static std::string ToStr(const T & v)
{
    std::ostringstream oss;
    oss << v;
    return oss.str();
}

std::string DumpHex(const char * buf, unsigned int len)
{
    static const char * c = "0123456789ABCDEF";
    std::string ret;
    for (unsigned int i = 0; i < len; ++i)
    {
        ret += c[((unsigned char)buf[i]) >> 4];
        ret += c[buf[i] & 0xf];
        if (i != len - 1)
            ret += ' ';
    }
    return ret;
}

static void FormatCurrentTime(TimeStamp now, char * time)
{
    time_t tm = (time_t)(now / 1000);
    strftime(time, 20, "%Y-%m-%d %H:%M:%S", localtime(&tm));
    time[19] = '.';
    snprintf(time + 20, 4, "%03llu", now % 1000);
}

static unsigned int GetTid()
{
    return (unsigned int)syscall(SYS_gettid);
}

__thread std::string * thread_name = NULL;

void SetThreadName(const std::string & name)
{
    if (thread_name)
        delete thread_name;
    thread_name = new std::string(name);
}

void UnsetThreadName()
{
    delete thread_name;
    thread_name = NULL;
}

void Logger::Log(unsigned int level, const char * file, int line, const char * function, const char * format, ...)
{
    if (level < level_) return;

    unsigned int tid = GetTid();

    lock_.Lock();

    char time[64];
    FormatCurrentTime(clock_->Now(), time);

    static const char * level_desc [] = {
        "TRACE", "DEBUG", "INFO", "NOTICE", "WARNING", "ERROR", "CRITICAL"
    };
    printf("[%s]\t[%s]\t", time, level_desc[level]);
    if (thread_name)
        printf("[%s:%u]\t", thread_name->c_str(), tid);
    else
        printf("[%u]\t", tid);
    printf("[%s:%d]\t[%s]\t", file, line, function);

    va_list args;
    va_start(args, format);
    vprintf(format, args);
    va_end(args);

    fflush(stdout);

    lock_.Unlock();
}

namespace paxos
{

void * RunPaxos(void * arg);

struct MembershipChange
{
    // Add
    MembershipChange(unsigned int id, const NodeInfo & node)
        : id_(id), node_(new NodeInfo(node)) { }
    // Delete
    MembershipChange(unsigned int id) : id_(id), node_(NULL) { }
    ~MembershipChange()
    {
        if (node_) delete node_;
    }

    bool operator==(const MembershipChange & m)
    {
        return id_ == m.id_ && (node_ && m.node_ ? *node_ == *m.node_ : node_ == m.node_);
    }

    unsigned int id_;
    NodeInfo * node_;
};

struct ProposedValue
{
    ProposedValue() : membership_change_(NULL) { }

    ProposedValue(const std::string & value, Callback * cb)
        : membership_change_(NULL), value_(value), cb_(cb) { }
    ProposedValue(unsigned int id, const NodeInfo & node, Callback * cb)
        : membership_change_(new MembershipChange(id, node)), cb_(cb) { }
    ProposedValue(unsigned int id, Callback * cb)
        : membership_change_(new MembershipChange(id)), cb_(cb) { }
    ProposedValue(const ProposedValue & p)
        : membership_change_(p.membership_change_ ? new MembershipChange(*p.membership_change_) : NULL),
            value_(p.value_), cb_(p.cb_) { }

    ~ProposedValue()
    {
        if (membership_change_) delete membership_change_;
    }

    MembershipChange * membership_change_;

    std::string value_;
    Callback * cb_;
};

struct PrepareMsg;
struct PrepareReplyMsg;
struct RejectMsg;
struct AcceptMsg;
struct AcceptReplyMsg;
struct CommitMsg;
struct CommitReplyMsg;

typedef unsigned long long ValueID;
typedef unsigned long long ProposalID;
typedef unsigned long long InstanceID;
typedef unsigned long long AcceptingID;
typedef unsigned long long CommittingID;

class PrepareRetryTimeout;
class AcceptRetryTimeout;
class CommitRetryTimeout;

class AcceptingValues;
class CommittingValues;

struct Value
{
    Value() : membership_change_(NULL) { }
    Value(unsigned int proposer, ValueID value_id)
        : proposer_(proposer), value_id_(value_id), noop_(true), membership_change_(NULL) { }
    Value(unsigned int proposer, ValueID value_id, const MembershipChange & membership_change)
        : proposer_(proposer), value_id_(value_id), noop_(false), membership_change_(new MembershipChange(membership_change)) { }
    Value(unsigned int proposer, ValueID value_id, const std::string & value)
        : proposer_(proposer), value_id_(value_id), noop_(false), membership_change_(NULL), value_(value) { }
    Value(const Value & v)
        : proposer_(v.proposer_), value_id_(v.value_id_),
          noop_(v.noop_),
          membership_change_(v.membership_change_ ? new MembershipChange(*v.membership_change_) : NULL),
          value_(v.value_) { }

    ~Value()
    {
        if (membership_change_) delete membership_change_;
    }

    bool operator==(const Value & v) const
    {
        return proposer_ == v.proposer_ && value_id_ == v.value_id_
            && noop_ == v.noop_
            && (membership_change_ && v.membership_change_ ? *membership_change_ == *v.membership_change_ : membership_change_ == v.membership_change_)
            && value_ == v.value_;
    }

    // The initial proposer
    unsigned int proposer_;
    ValueID value_id_;
    // No-op is required to ensure the order
    bool noop_;
    MembershipChange * membership_change_;
    std::string value_;
};

static std::string Debug(const Value & v, const StateMachine * sm)
{
    return "(" + ToStr(v.proposer_) + ":" + ToStr(v.value_id_) + ")" + (!v.noop_
        ? v.membership_change_
            ? std::string("m") + (v.membership_change_->node_
                ? "+" + ToStr(v.membership_change_->id_) + "=" + v.membership_change_->node_->ip_ + ":" + ToStr(v.membership_change_->node_->port_)
                : "-" + ToStr(v.membership_change_->id_))
            : "+" + sm->Debug(v.value_)
        : "-");
}

struct AcceptedValue
{
    AcceptedValue() { }
    AcceptedValue(ProposalID proposal_id, const Value & value)
        : proposal_id_(proposal_id), value_(value) { }
    AcceptedValue(ProposalID proposal_id, unsigned int proposer, ValueID value_id)
        : proposal_id_(proposal_id), value_(proposer, value_id) { }
    AcceptedValue(ProposalID proposal_id, unsigned int proposer, ValueID value_id, const MembershipChange & membership_change)
        : proposal_id_(proposal_id), value_(proposer, value_id, membership_change) { }
    AcceptedValue(ProposalID proposal_id, unsigned int proposer, ValueID value_id, const std::string & value)
        : proposal_id_(proposal_id), value_(proposer, value_id, value) { }
    AcceptedValue(const AcceptedValue & v)
        : proposal_id_(v.proposal_id_), value_(v.value_) { }

    bool operator==(const AcceptedValue & v) const
    {
        return proposal_id_ == v.proposal_id_ && value_ == v.value_;
    }

    ProposalID proposal_id_;
    Value value_;
};

static std::string Debug(const AcceptedValue & v, const StateMachine * sm)
{
    return "<" + ToStr(v.proposal_id_) + ">" + Debug(v.value_, sm);
}

struct AvailableInstanceIDs
{
public:
    AvailableInstanceIDs(Logger * logger)
        : logger_(logger)
    {
        ids_.insert(std::make_pair(0, (InstanceID)-1));
    }

    AvailableInstanceIDs & operator=(const AvailableInstanceIDs & ids)
    {
        logger_ = ids.logger_;
        ids_ = ids.ids_;
        return *this;
    }

    InstanceID Next()
    {
        InstanceID a = ids_.begin()->first;
        Remove(a);
        return a;
    }

    bool Contains(InstanceID id)
    {
        std::set<std::pair<InstanceID, InstanceID> >::iterator after = ids_.upper_bound(std::make_pair(id, (InstanceID)-1));
        if (after == ids_.begin()) return false;
        std::set<std::pair<InstanceID, InstanceID> >::iterator cur = --after;
        return cur->first <= id && cur->second > id;
    }

    void Remove(InstanceID id)
    {
        std::set<std::pair<InstanceID, InstanceID> >::iterator after = ids_.upper_bound(std::make_pair(id, (InstanceID)-1));
        if (after == ids_.begin())
            ERROR(logger_, "remove id %llu failed\n", id);
        ASSERT(logger_, after != ids_.begin());
        std::set<std::pair<InstanceID, InstanceID> >::iterator cur = --after;
        ASSERT(logger_, cur->first <= id && cur->second > id);

        InstanceID a = cur->first;
        InstanceID b = cur->second;
        ids_.erase(cur);
        if (a != id)
            ids_.insert(std::make_pair(a, id));
        if (id + 1 != b)
            ids_.insert(std::make_pair(id + 1, b));
    }

    std::string ToString() const
    {
        std::string s;
        for (std::set<std::pair<InstanceID, InstanceID> >::const_iterator it = ids_.begin();
                it != ids_.end();
                ++it)
        {
            if (s.length()) s+= ", ";
            s += "[" + ToStr(it->first) + ", " + ToStr(it->second) + ")";
        }
        return s;
    }

    Logger * logger_;

    std::set<std::pair<InstanceID, InstanceID> > ids_;
};

class PaxosImpl
{
public:
    PaxosImpl(Logger * logger, const std::string & thread_prefix,
                Clock * clock, Timer * timer, Rand * rand,
                const NodeInfoMap & nodes, unsigned int index,
                NetWork * net, StateMachine * sm,
                const Paxos::Config & config,
                unsigned int * whole_system_reference_count_for_debugging)
        : logger_(logger),
            thread_prefix_(thread_prefix), clock_(clock),
            timer_(timer), rand_(rand),
            nodes_(nodes), index_(index),
            net_(net), sm_(sm), config_(config),
            stop_(false),
            value_id_(0),
            uncommitted_instance_ids_(logger_),
            preparing_instance_ids_(logger_),
            unproposed_instance_ids_(logger_),
            max_proposal_id_(0), proposal_count_(0), proposal_id_(0), prepare_retry_timeout_(NULL), accepting_id_(0),
            promised_proposal_id_(0), committing_id_(0),
            next_id_to_apply_(0),
            whole_system_reference_count_for_debugging_(whole_system_reference_count_for_debugging)
    {
        net_->Init(this);
        ASSERT(logger_, pthread_create(&thread_, NULL, RunPaxos, this) == 0);
    }

    ~PaxosImpl()
    {
        AtomicSet(&stop_, true);
        ASSERT(logger_, pthread_join(thread_, NULL) == 0);
        net_->Fini();
    }

    void OnReceiveMessage(const std::string & msg)
    {
        received_.Put(msg);
    }

    void Propose(const std::string & value, Callback * cb)
    {
        proposed_.Put(ProposedValue(value, cb));
    }

#if 0
    void AddMember(unsigned int id, const NodeInfo & node, Callback * cb)
    {
        proposed_.Put(ProposedValue(id, node, cb));
    }

    void DelMember(unsigned int id, Callback * cb)
    {
        proposed_.Put(ProposedValue(id, cb));
    }
#endif

    void Loop();

private:
    // For proposer
    void UpdateProposalID();

    friend class PrepareDelay;
    friend class PrepareRetryTimeout;
    void Prepare();

    void StartPrepare();
    void RestartPrepare();

    void OnPrepareReply(const PrepareReplyMsg * msg);
    void UpdateByPreAcceptedValues(const std::map<InstanceID, AcceptedValue> * values);

    void OnReject(const RejectMsg * msg);

    friend class AcceptRetryTimeout;
    void Accept(const AcceptingValues * accept);

    void AcceptRejected();

    // For acceptor
    void OnPrepare(const PrepareMsg * msg);

    void FilterAcceptedValues(const AvailableInstanceIDs * ids,
                                std::map<InstanceID, AcceptedValue> * values) const;

    void OnAccept(const AcceptMsg * msg);
    void OnAcceptReply(const AcceptReplyMsg * msg);

    void Propose(const ProposedValue & proposed);

    friend class CommitRetryTimeout;
    void Commit(CommittingValues * commit);

    void OnCommit(const CommitMsg * msg);
    void OnCommitReply(const CommitReplyMsg * msg);

private:
    Logger * logger_;
    std::string thread_prefix_;
    Clock * clock_;
    Timer * timer_;
    Rand * rand_;

    NodeInfoMap nodes_;
    unsigned int index_;

    NetWork * net_;
    StateMachine * sm_;

    Paxos::Config config_;

    bool stop_;
    Queue<std::string> received_;
    Queue<ProposedValue> proposed_;
    pthread_t thread_;

    // For proposer
    ValueID value_id_;
    // Node index + ValueID uniquely define the proposed value
    std::map<ValueID, ProposedValue> uncommitted_proposed_values_;

    // Use a copy(not strictly, see below) of uncommitted instance
    // ids(unproposed instance ids) at the beginning(it still needs to be
    // updated in OnCommit) of preparing instead of the newest ids because if
    // keey using newest uncommitted ids every time preparing, it would also
    // be better to use newset ids for proposing(accepting), then it would be
    // better to remove those on going accept requests that use
    // newly committed instance ids. Mixing these two scenarios is
    // complicate. (See OnCommit for more about updating unproposed ids by
    // commit)
    // To keep it simple, we always use the copy for prepare, prepare retry,
    // propose(accept), propose retry, and update the copy only when
    // new prepare
    // It's not dangerous to prepare for committed values, since when succeed,
    // the pre accepted value must be the same as the committed value;
    // It's also not dangerous to propose for committed values, either
    // the proposed value is the committed value or the proposal will be
    // rejected (because accept means majority are promised on the proposal id,
    // and if the value differs, then majority must have a greator proposal id);

    // Unproposed ids are not strictly See OnCommit for more

    // TODO [check] check if these three can be one

    // Uncommitted instance ids are used for prepare
    AvailableInstanceIDs uncommitted_instance_ids_;

    // TODO add comments and check when to clear and combine these two
    AvailableInstanceIDs preparing_instance_ids_;

    // Unproposed instance ids are used for propose(accept)
    AvailableInstanceIDs unproposed_instance_ids_;

    ProposalID max_proposal_id_;

    unsigned long long proposal_count_;
    ProposalID proposal_id_;
    PrepareRetryTimeout * prepare_retry_timeout_;
    std::set<unsigned int> prepare_promised_;

    // Since a brand new proposal is created after reject,
    // a different value may be proposed with a used id
    std::map<InstanceID, ValueID> initial_proposals_;
    std::set<ValueID> newly_proposed_values_;

    std::map<InstanceID, AcceptedValue> pre_accepted_values_;

    AcceptingID accepting_id_;
    std::map<AcceptingID, AcceptingValues *> accepting_values_;

    // For acceptor
    ProposalID promised_proposal_id_;
    std::map<InstanceID, AcceptedValue> accepted_values_;

    CommittingID committing_id_;
    std::map<CommittingID, CommittingValues *> committing_values_;

    // For learner
    std::map<InstanceID, AcceptedValue> committed_values_;

    // For executor
    InstanceID next_id_to_apply_;

private:
    void SystemInc(const char * caller)
    {
        if (whole_system_reference_count_for_debugging_)
            TRACE(logger_, "system ref inc by %s, result in %u\n", caller,
                    AtomicInc(whole_system_reference_count_for_debugging_));
    }

    void SystemDec(const char * caller)
    {
        if (whole_system_reference_count_for_debugging_)
            TRACE(logger_, "system ref dec by %s, result in %u\n", caller,
                    AtomicDec(whole_system_reference_count_for_debugging_));
    }

    // Let the outter know when is safe to shut down the whole system
    unsigned int * whole_system_reference_count_for_debugging_;
};

static unsigned int CalcAvailableInstanceIDsLength(const AvailableInstanceIDs * ids)
{
    return ids->ids_.size() * 2 * sizeof (InstanceID);
}

static void FillAvailableInstanceIDs(char * buf, const AvailableInstanceIDs * ids)
{
    unsigned int cur = 0;
    for (std::set<std::pair<InstanceID, InstanceID> >::const_iterator it = ids->ids_.begin();
            it != ids->ids_.end();
            ++it)
    {
        *(InstanceID *)(buf + cur) = it->first;
        *(InstanceID *)(buf + cur + sizeof (InstanceID)) = it->second;
        cur += 2 * sizeof (InstanceID);
    }
}

static void ExtractAvailableInstanceIDs(Logger * logger, const char * buf, unsigned int len, AvailableInstanceIDs * ids)
{
    ids->ids_.clear();

    unsigned int cur = 0;
    while (cur != len)
    {
        InstanceID a = *(const InstanceID *)(buf + cur);
        InstanceID b = *(const InstanceID *)(buf + cur + sizeof (InstanceID));
        cur += 2 * sizeof (InstanceID);

        ASSERT(logger, ids->ids_.insert(std::make_pair(a, b)).second);
    }
}

static unsigned int CalcValueLength(const Value * val)
{
    return sizeof (unsigned int) + sizeof (ValueID) + sizeof (bool) + (!val->noop_
        ? sizeof (bool) + (val->membership_change_
            ? sizeof (unsigned int) + sizeof (bool) + (val->membership_change_->node_
                ? sizeof (unsigned int) + val->membership_change_->node_->ip_.size() + sizeof (unsigned short)
                : 0)
            : sizeof (unsigned int) + val->value_.size())
        : 0);
}

static void FillValue(char * buf, unsigned int & cur, const Value * val)
{
    *(unsigned int *)(buf + cur) = val->proposer_;
    *(ValueID *)(buf + (cur += sizeof (unsigned int))) = val->value_id_;
    *(bool *)(buf + (cur += sizeof (ValueID))) = val->noop_;
    if (!val->noop_)
    {
        *(bool *)(buf + (cur += sizeof (bool))) = val->membership_change_;
        if (val->membership_change_)
        {
            MembershipChange * change = val->membership_change_;
            *(unsigned int *)(buf + (cur += sizeof (bool))) = change->id_;
            *(bool *)(buf + (cur += sizeof (unsigned int))) = change->node_;
            if (change->node_)
            {
                *(unsigned int *)(buf + (cur += sizeof (bool))) = change->node_->ip_.size();
                memcpy(buf + (cur += sizeof (unsigned int)), change->node_->ip_.c_str(), change->node_->ip_.size());
                *(unsigned short *)(buf + (cur += change->node_->ip_.size())) = change->node_->port_;
                cur += sizeof (unsigned short);
            }
            else
                cur += sizeof (bool);
        }
        else
        {
            *(unsigned int *)(buf + (cur += sizeof (bool))) = val->value_.size();
            memcpy(buf + (cur += sizeof (unsigned int)), val->value_.c_str(), val->value_.size());
            cur += val->value_.size();
        }
    }
    else
        cur += sizeof (bool);
}

static Value ExtractValue(const char * buf, unsigned int & cur)
{
    unsigned int proposer = *(const unsigned int *)(buf + cur);
    ValueID value_id = *(const ValueID *)(buf + (cur += sizeof (unsigned int)));
    bool noop = *(const bool *)(buf + (cur += sizeof (ValueID)));
    if (!noop)
    {
        bool membership = *(const bool *)(buf + (cur += sizeof (bool)));
        if (membership)
        {
            unsigned int node_id = *(const unsigned int *)(buf + (cur += sizeof (bool)));
            bool add = *(const bool *)(buf + (cur += sizeof (unsigned int)));
            if (add)
            {
                unsigned int len = *(const unsigned int *)(buf + (cur += sizeof (bool)));
                std::string ip = std::string(buf + (cur += sizeof (unsigned int)), len);
                unsigned short port = *(const unsigned short *)(buf + (cur += len));
                cur += sizeof (unsigned short);

                return Value(proposer, value_id, MembershipChange(node_id, NodeInfo(ip, port)));
            }
            else
            {
                cur += sizeof (bool);

                return Value(proposer, value_id, MembershipChange(node_id));
            }
        }
        else
        {
            unsigned int len = *(const unsigned int *)(buf + (cur += sizeof (bool)));
            std::string value = std::string(buf + (cur += sizeof (unsigned int)), len);
            cur += len;

            return Value(proposer, value_id, value);
        }
    }
    else
    {
        cur += sizeof (bool);

        return Value(proposer, value_id);
    }
}

static unsigned int CalcAcceptedValuesLength(const std::map<InstanceID, AcceptedValue> * values)
{
    unsigned int len = (sizeof (InstanceID) + sizeof (ProposalID)) * values->size();
    for (std::map<InstanceID, AcceptedValue>::const_iterator it = values->begin();
            it != values->end();
            ++it)
        len += CalcValueLength(&it->second.value_);
    return len;
}

static void FillAcceptedValues(char * buf, const std::map<InstanceID, AcceptedValue> * values)
{
    unsigned int cur = 0;
    for (std::map<InstanceID, AcceptedValue>::const_iterator it = values->begin();
            it != values->end();
            ++it)
    {
        *(InstanceID *)(buf + cur) = it->first;
        *(ProposalID *)(buf + (cur += sizeof (InstanceID))) = it->second.proposal_id_;
        FillValue(buf, cur += sizeof (ProposalID), &it->second.value_);
    }
}

static void ExtractAcceptedValues(Logger * logger, const char * buf, unsigned int len, std::map<InstanceID, AcceptedValue> * values)
{
    unsigned int cur = 0;
    while (cur != len)
    {
        InstanceID id = *(const InstanceID *)(buf + cur);
        ProposalID proposal_id = *(const ProposalID *)(buf + (cur += sizeof (InstanceID)));
        
        ASSERT(logger, values->insert(std::make_pair(id, AcceptedValue(proposal_id, ExtractValue(buf, cur += sizeof (ProposalID))))).second);
    }
}

static unsigned int CalcInstanceValuesLength(const std::map<InstanceID, Value> * values)
{
    unsigned int len = sizeof (InstanceID) * values->size();
    for (std::map<InstanceID, Value>::const_iterator it = values->begin();
            it != values->end();
            ++it)
        len += CalcValueLength(&it->second);
    return len;
}

static void FillInstanceValues(char * buf, const std::map<InstanceID, Value> * values)
{
    unsigned int cur = 0;
    for (std::map<InstanceID, Value>::const_iterator it = values->begin();
            it != values->end();
            ++it)
    {
        *(InstanceID *)(buf + cur) = it->first;
        FillValue(buf, cur += sizeof (InstanceID), &it->second);
    }
}

static void ExtractInstanceValues(Logger * logger, const char * buf, unsigned int len, std::map<InstanceID, Value> * values)
{
    unsigned int cur = 0;
    while (cur != len)
    {
        InstanceID id = *(const InstanceID *)(buf + cur);
        ASSERT(logger, values->insert(std::make_pair(id, ExtractValue(buf, cur += sizeof (InstanceID)))).second);
    }
}

class PrepareDelay : public Timeout
{
public:
    PrepareDelay(PaxosImpl * paxos) : paxos_(paxos)
    {
        paxos_->SystemInc(__FUNCTION__);
    }
    virtual ~PrepareDelay()
    {
        paxos_->SystemDec(__FUNCTION__);
    }

    virtual void Process()
    {
        if (!canceled_)
            paxos_->Prepare();
        delete this;
    }

private:
    PaxosImpl * paxos_;
};

static unsigned int GetMsgType(const char * msg)
{
    return *(unsigned int *)msg;
}

static const unsigned int MSG_PREPARE = 0;
#pragma pack(push, 1)
struct PrepareMsg
{
    PrepareMsg(unsigned int proposer, ProposalID id, unsigned int len)
        : type_(MSG_PREPARE), proposer_(proposer), id_(id), len_(len) { }

    unsigned int type_;
    unsigned int proposer_;
    ProposalID id_;

    unsigned int len_;
    char instance_ids_[0];
};
#pragma pack(pop)

class PrepareRetryTimeout : public Timeout
{
public:
    PrepareRetryTimeout(PaxosImpl * paxos, unsigned int count)
        : paxos_(paxos), count_(count)
    {
        paxos_->SystemInc(__FUNCTION__);
    }
    virtual ~PrepareRetryTimeout()
    {
        paxos_->SystemDec(__FUNCTION__);
    }

    virtual void Process()
    {
        if (!canceled_)
        {
            if (--count_ == 0)
            {
                paxos_->RestartPrepare();
                delete this;
            }
            else
                paxos_->Prepare();
        }
        else
            delete this;
    }

private:
    PaxosImpl * paxos_;

    unsigned int count_;
};

void PaxosImpl::UpdateProposalID()
{
    // TODO [check] check if it's possible
    // proposal_id_ >= max_proposal_id_ already
    proposal_id_ = ((++proposal_count_) << 16) | index_;
    while (proposal_id_ < max_proposal_id_)
        proposal_id_ = ((++proposal_count_) << 16) | index_;
}

void PaxosImpl::RestartPrepare()
{
    prepare_retry_timeout_ = NULL;
    prepare_promised_.clear();
    pre_accepted_values_.clear();
    StartPrepare();
}

void PaxosImpl::Prepare()
{
    // TODO [opt] move message generation out of retry

    std::string dmp = preparing_instance_ids_.ToString();
    DEBUG(logger_, "broadcast prepare: %s\n", dmp.c_str());

    unsigned int len = CalcAvailableInstanceIDsLength(&preparing_instance_ids_);
    unsigned int size = sizeof (PrepareMsg) + len;

    PrepareMsg * msg = new (new char[size]) PrepareMsg(index_, proposal_id_, len);
    FillAvailableInstanceIDs(msg->instance_ids_, &preparing_instance_ids_);
    std::string m((const char *)msg, size);
    delete [] (char *)msg;

    for (NodeInfoMap::iterator it = nodes_.begin(); it != nodes_.end(); ++it)
        net_->SendMessageUDP(it->second.ip_, it->second.port_, m);

    timer_->AddTimeout(prepare_retry_timeout_, clock_->Now() + config_.prepare_retry_timeout_);
}

static const unsigned int MSG_PREPARE_REPLY = 1;
#pragma pack(push, 1)
struct PrepareReplyMsg
{
    PrepareReplyMsg(unsigned int acceptor, ProposalID id, unsigned int len)
        : type_(MSG_PREPARE_REPLY), acceptor_(acceptor), id_(id), len_(len) { }

    unsigned int type_;
    unsigned int acceptor_;
    ProposalID id_;

    unsigned int len_;
    char values_[0];
};
#pragma pack(pop)

static const unsigned int MSG_REJECT = 2;
#pragma pack(push, 1)
struct RejectMsg
{
    RejectMsg(ProposalID max_id)
        : type_(MSG_REJECT), max_id_(max_id) { }

    unsigned int type_;
    ProposalID max_id_;
};
#pragma pack(pop)

void PaxosImpl::OnPrepare(const PrepareMsg * msg)
{
    DEBUG(logger_, "proposal id: %llu, promised proposal id: %llu\n", msg->id_, promised_proposal_id_);

    if (msg->id_ > max_proposal_id_)
        max_proposal_id_ = msg->id_;

    if (msg->id_ > promised_proposal_id_)
    {
        promised_proposal_id_ = msg->id_;

        AvailableInstanceIDs instance_ids(logger_);
        ExtractAvailableInstanceIDs(logger_, msg->instance_ids_, msg->len_, &instance_ids);

        std::map<InstanceID, AcceptedValue> values;
        FilterAcceptedValues(&instance_ids, &values);

        std::string dmp;
        for (std::map<InstanceID, AcceptedValue>::iterator it = values.begin();
                it != values.end();
                ++it)
        {
            if (dmp.length()) dmp += ", ";
            dmp += "[" + ToStr(it->first) + "] = " + Debug(it->second, sm_);
        }
        DEBUG(logger_, "reply prepare to %u: %s\n", msg->proposer_, dmp.c_str());

        unsigned int len = CalcAcceptedValuesLength(&values);
        unsigned int size = sizeof (PrepareReplyMsg) + len;
        PrepareReplyMsg * reply = new (new char[size]) PrepareReplyMsg(index_, msg->id_, len);
        FillAcceptedValues(reply->values_, &values);
        std::string r((const char *)reply, size);
        delete [] (char *)reply;

        net_->SendMessageUDP(nodes_[msg->proposer_].ip_, nodes_[msg->proposer_].port_, r);
    }
    else if (msg->id_ < promised_proposal_id_)
    {
        RejectMsg reject(max_proposal_id_);
        std::string r((const char *)&reject, sizeof reject);
        net_->SendMessageUDP(nodes_[msg->proposer_].ip_, nodes_[msg->proposer_].port_, r);
    }
}

static void FilterAcceptedInstances(Logger * logger, const std::map<InstanceID, AcceptedValue> * values,
                                    InstanceID a, InstanceID b,
                                    std::map<InstanceID, AcceptedValue> * result)
{
    for (std::map<InstanceID, AcceptedValue>::const_iterator it = values->lower_bound(a);
            it != values->lower_bound(b);
            ++it)
        ASSERT(logger, result->insert(*it).second);
}

void PaxosImpl::FilterAcceptedValues(const AvailableInstanceIDs * ids,
                                        std::map<InstanceID, AcceptedValue> * values) const
{
    for (std::set<std::pair<InstanceID, InstanceID> >::const_iterator it = ids->ids_.begin();
            it != ids->ids_.end();
            ++it)
    {
        FilterAcceptedInstances(logger_, &accepted_values_, it->first, it->second, values);
        FilterAcceptedInstances(logger_, &committed_values_, it->first, it->second, values);
    }
}

struct AcceptingValues
{
    AcceptingValues(Logger * logger, AcceptingID id) : logger_(logger), id_(id), retry_timeout_(NULL) { }

    void AddValue(InstanceID id, const Value & value)
    {
        ASSERT(logger_, values_.insert(std::make_pair(id, value)).second);
    }

#if 0
    void AddValue(InstanceID id, unsigned int proposer, ValueID value_id)
    {
        AddValue(id, Value(proposer, value_id));
    }

    void AddValue(InstanceID id, unsigned int proposer, ValueID value_id,
                   const std::string & value)
    {
        AddValue(id, Value(proposer, value_id, value));
    }
#endif

    Logger * logger_;

    AcceptingID id_;
    std::map<InstanceID, Value> values_;

    std::set<unsigned int> accepted_;

    AcceptRetryTimeout * retry_timeout_;
};

class AcceptRetryTimeout : public Timeout
{
public:
    AcceptRetryTimeout(PaxosImpl * paxos, AcceptingValues * values, unsigned int count)
        : paxos_(paxos), values_(values), count_(count)
    {
        paxos_->SystemInc(__FUNCTION__);
    }
    virtual ~AcceptRetryTimeout()
    {
        paxos_->SystemDec(__FUNCTION__);
    }

    virtual void Process()
    {
        if (!canceled_)
        {
            if (--count_ == 0)
            {
                paxos_->AcceptRejected();
                delete this;
            }
            else
                paxos_->Accept(values_);
        }
        else
            delete this;
    }

private:
    PaxosImpl * paxos_;
    AcceptingValues * values_;
    unsigned int count_;
};

struct CommittingValues
{
    // Accepting values don't need proposal id because
    // when id becomes invalid, all the on-going accept requests are canceled,
    // which is not what happened to committing values
    CommittingValues(CommittingID id, ProposalID proposal_id,
            const std::map<InstanceID, Value> & values)
        : id_(id), proposal_id_(proposal_id), values_(values), retry_timeout_(NULL) { }

    CommittingID id_;
    ProposalID proposal_id_;
    std::map<InstanceID, Value> values_;

    std::set<unsigned int> replied_;

    CommitRetryTimeout * retry_timeout_;
};

class CommitRetryTimeout : public Timeout
{
public:
    CommitRetryTimeout(PaxosImpl * paxos, CommittingValues * values)
        : paxos_(paxos), values_(values)
    {
        paxos_->SystemInc(__FUNCTION__);
    }
    virtual ~CommitRetryTimeout()
    {
        paxos_->SystemDec(__FUNCTION__);
    }

    virtual void Process()
    {
        if (!canceled_)
            paxos_->Commit(values_);
        else
            delete this;
    }

private:
    PaxosImpl * paxos_;

    CommittingValues * values_;
};

void PaxosImpl::OnPrepareReply(const PrepareReplyMsg * msg)
{
    if (!prepare_retry_timeout_ || msg->id_ != proposal_id_) return;

    ASSERT(logger_, nodes_.find(msg->acceptor_) != nodes_.end());
    prepare_promised_.insert(msg->acceptor_);

    std::map<InstanceID, AcceptedValue> values;
    ExtractAcceptedValues(logger_, msg->values_, msg->len_, &values);
    UpdateByPreAcceptedValues(&values);

    if (prepare_promised_.size() >= nodes_.size() / 2 + 1)
    {
        prepare_promised_.clear();

        prepare_retry_timeout_->Cancel();
        prepare_retry_timeout_ = NULL;

        ASSERT(logger_, accepting_values_.empty());

        unproposed_instance_ids_ = uncommitted_instance_ids_;

        // Values to be accepted include
        // non-conflict values initially proposed by us,
        // newly proposed values and pre accepted values (ours
        // required, others optional?)
        // other uncommitted values should not be proposed
        // again (with different instance id) until it's initially
        // assigned instance id
        // is committed with different value as these values
        // might be accepted and proposed by others
        AcceptingValues * accept = NULL;

        // This can be more strict by using first uncommitted value
        // when the prepare request is generated
        for (std::map<InstanceID, AcceptedValue>::iterator it = pre_accepted_values_.begin();
                it != pre_accepted_values_.end();
                ++it)
        {
            // Don't compare to committed value if instance id is committed
            // as pre accepted values may not be up to date and will be rejected
            // anyway

            if (it->second.value_.proposer_ == index_)
                // Values pre accepted by others can't be new
                ASSERT(logger_, newly_proposed_values_.find(it->second.value_.value_id_) == newly_proposed_values_.end());

            // [OUTDATED] Don't have to remove committed values since
            // for those out-of-date values, other acceptors will reject
            // for those up-to-date values, other acceptors can simply
            // ignore as paxos ensure they are same

            // Due to id is removed in OnCommit, unproposed_instance_ids_
            // may not contain the pre-accepted value

            if (unproposed_instance_ids_.Contains(it->first))
            {
                if (!accept)
                {
                    accept = new AcceptingValues(logger_, ++accepting_id_);
                    accepting_values_.insert(std::make_pair(accepting_id_, accept));
                }

                unproposed_instance_ids_.Remove(it->first);
                accept->AddValue(it->first, it->second.value_);
            }
        }

        pre_accepted_values_.clear();

        // We need to fill the gaps with no-op to ensure the order that newly proposed
        // values can't go before already committed values
        // and this needs to go before values initially proposed by us as these values
        // can still be the values proposed after the committed values when we are very
        // behind
        // The requirement is new proposal can't be accepted after commit. If it is
        // porposed to the server same as the committed values, there must be no holes
        // as holes are filled when it passes the preparing and the instance ids are
        // consumed in order.
        // If it is proposed to other servers, the server must have a proposal id
        // lower than the one of committed server and the holes will be filled here
        while (unproposed_instance_ids_.ids_.size() != 1)
        {
            std::set<std::pair<InstanceID, InstanceID> >::iterator it = unproposed_instance_ids_.ids_.begin();
            for (InstanceID id = it->first; id != it->second; ++id)
            {
                if (!accept)
                {
                    accept = new AcceptingValues(logger_, ++accepting_id_);
                    accepting_values_.insert(std::make_pair(accepting_id_, accept));
                }
                accept->AddValue(id, Value(index_, ++value_id_));
            }
            unproposed_instance_ids_.ids_.erase(it);
        }

        // We need to propose those values initially proposed by us and
        // not presented in the others' pre-accepted values
        // This is possible when the initial propose is already too
        // old(the proposal id is too small) and no one else propose again
        for (std::map<InstanceID, ValueID>::iterator it = initial_proposals_.begin();
                it != initial_proposals_.end();
                ++it)
            if (unproposed_instance_ids_.Contains(it->first))
            {
                if (!accept)
                {
                    accept = new AcceptingValues(logger_, ++accepting_id_);
                    accepting_values_.insert(std::make_pair(accepting_id_, accept));
                }

                // With this, initial proposals for a single instance id
                // won't be a set
                unproposed_instance_ids_.Remove(it->first);
                ASSERT(logger_, uncommitted_proposed_values_.find(it->second) != uncommitted_proposed_values_.end());
                const ProposedValue * proposed = &uncommitted_proposed_values_[it->second];
                accept->AddValue(it->first, !proposed->membership_change_
                    ? Value(index_, it->second, proposed->value_)
                    : Value(index_, it->second, *proposed->membership_change_));
            }

        for (std::set<ValueID>::iterator it = newly_proposed_values_.begin();
                it != newly_proposed_values_.end();
                ++it)
        {
            if (!accept)
            {
                accept = new AcceptingValues(logger_, ++accepting_id_);
                accepting_values_.insert(std::make_pair(accepting_id_, accept));
            }

            InstanceID instance_id = unproposed_instance_ids_.Next();
            ASSERT(logger_, initial_proposals_.insert(std::make_pair(instance_id, *it)).second);

            ASSERT(logger_, uncommitted_proposed_values_.find(*it) != uncommitted_proposed_values_.end());
            const ProposedValue * proposed = &uncommitted_proposed_values_[*it];
            accept->AddValue(instance_id, !proposed->membership_change_
                ? Value(index_, *it, proposed->value_)
                : Value(index_, *it, *proposed->membership_change_));
        }
        newly_proposed_values_.clear();

        if (accept)
        {
            accept->retry_timeout_ = new AcceptRetryTimeout(this, accept, config_.accept_retry_count_);
            Accept(accept);
        }

        if (!committed_values_.empty())
        {
            std::map<InstanceID, Value> values;
            for (std::map<InstanceID, AcceptedValue>::iterator it = committed_values_.begin();
                    it != committed_values_.end();
                    ++it)
                values.insert(std::make_pair(it->first, it->second.value_));

            CommittingValues * commit = new CommittingValues(++committing_id_, proposal_id_, values);
            committing_values_.insert(std::make_pair(committing_id_, commit));

            commit->retry_timeout_ = new CommitRetryTimeout(this, commit);
            Commit(commit);
        }
    }
}

void PaxosImpl::UpdateByPreAcceptedValues(const std::map<InstanceID, AcceptedValue> * values)
{
    std::string dmp;
    for (std::map<InstanceID, AcceptedValue>::const_iterator it = values->begin();
            it != values->end();
            ++it)
    {
        if (dmp.length()) dmp += ", ";
        dmp += "[" + ToStr(it->first) + "] = " + Debug(it->second, sm_);
    }
    DEBUG(logger_, "update by pre-accepted values: %s\n", dmp.c_str());

    for (std::map<InstanceID, AcceptedValue>::const_iterator it = values->begin();
            it != values->end();
            ++it)
        if (pre_accepted_values_.find(it->first) != pre_accepted_values_.end())
        {
            if (it->second.proposal_id_ > pre_accepted_values_[it->first].proposal_id_)
                pre_accepted_values_[it->first] = it->second;
        }
        else
            pre_accepted_values_.insert(*it);
}

void PaxosImpl::OnReject(const RejectMsg * msg)
{
    // Optimization and won't cause trouble when updating from
    // removed servers
    if (max_proposal_id_ < msg->max_id_)
        max_proposal_id_ = msg->max_id_;
}

void PaxosImpl::StartPrepare()
{
    ASSERT(logger_, prepare_retry_timeout_ == NULL);
    ASSERT(logger_, prepare_promised_.empty());
    ASSERT(logger_, pre_accepted_values_.empty());

    UpdateProposalID();

    preparing_instance_ids_ = uncommitted_instance_ids_;
    prepare_retry_timeout_ = new PrepareRetryTimeout(this, config_.prepare_retry_count_);

    TimeStamp now = clock_->Now();
    TimeStamp future = now + rand_->Randomize(config_.prepare_delay_min_, config_.prepare_delay_max_);
    DEBUG(logger_, "add restart prepare timer: now = %llu, future = %llu\n", now, future);
    timer_->AddTimeout(new PrepareDelay(this), future);
}

void PaxosImpl::Propose(const ProposedValue & proposed)
{
    std::string dmp = sm_->Debug(proposed.value_);
    INFO(logger_, "propose: %s\n", dmp.c_str()); // TODO

    uncommitted_proposed_values_.insert(std::make_pair(++value_id_, proposed));

    if (!prepare_retry_timeout_)
    {
        AcceptingValues * accept = new AcceptingValues(logger_, ++accepting_id_);
        accepting_values_.insert(std::make_pair(accepting_id_, accept));

        // To prevent committed value proposed again
        // Do not propose the value until the initially assigned instance id is commited
        // with different value
        // This is one place where assign the initial instance id
        InstanceID instance_id = unproposed_instance_ids_.Next();
        // Record value proposed by us, so if the final committed value differs,
        // we can propose again
        ASSERT(logger_, initial_proposals_.insert(std::make_pair(instance_id, value_id_)).second);

        accept->AddValue(instance_id, !proposed.membership_change_
            ? Value(index_, value_id_, proposed.value_)
            : Value(index_, value_id_, *proposed.membership_change_));
        accept->retry_timeout_ = new AcceptRetryTimeout(this, accept, config_.accept_retry_count_);
        Accept(accept);
    }
    else
        // This is the other place
        newly_proposed_values_.insert(value_id_);
}

static const unsigned int MSG_ACCEPT = 3;
#pragma pack(push, 1)
struct AcceptMsg
{
    AcceptMsg(unsigned int proposer, AcceptingID accept, ProposalID id, unsigned int len)
        : type_(MSG_ACCEPT), proposer_(proposer), accept_(accept), id_(id), len_(len) { }

    unsigned int type_;
    unsigned int proposer_;
    AcceptingID accept_;
    ProposalID id_;

    unsigned int len_;
    char values_[0];
};
#pragma pack(pop)

void PaxosImpl::Accept(const AcceptingValues * accept)
{
    std::string dmp;
    for (std::map<InstanceID, Value>::const_iterator it = accept->values_.begin();
            it != accept->values_.end();
            ++it)
    {
        if (dmp.length())
            dmp += ", ";
        dmp += "[" + ToStr(it->first) + "] = " + Debug(it->second, sm_);
    }
    DEBUG(logger_, "broadcast accept: %s\n", dmp.c_str());

    unsigned int len = CalcInstanceValuesLength(&accept->values_);
    unsigned int size = sizeof (AcceptMsg) + len;

    AcceptMsg * msg = new (new char[size]) AcceptMsg(index_, accept->id_, proposal_id_, len);
    FillInstanceValues(msg->values_, &accept->values_);
    //std::string d = DumpHex(msg->values_, len);
    //DEBUG(logger_, "len = %u, d = %s\n", len, d.c_str());
    std::string m((const char *)msg, size);
    delete [] (char *)msg;

    for (NodeInfoMap::iterator it = nodes_.begin(); it != nodes_.end(); ++it)
        net_->SendMessageUDP(it->second.ip_, it->second.port_, m);

    timer_->AddTimeout(accept->retry_timeout_, clock_->Now() + config_.accept_retry_timeout_);
}

void PaxosImpl::AcceptRejected()
{
    DEBUG(logger_, "accept rejected\n");

    StartPrepare();

    for (std::map<AcceptingID, AcceptingValues *>::iterator it = accepting_values_.begin();
            it != accepting_values_.end();
            ++it)
    {
        it->second->retry_timeout_->Cancel();
        delete it->second;
    }

    accepting_values_.clear();
}

static const unsigned int MSG_ACCEPT_REPLY = 4;
#pragma pack(push, 1)
struct AcceptReplyMsg
{
    AcceptReplyMsg(unsigned int acceptor, ProposalID id, AcceptingID accept)
        : type_(MSG_ACCEPT_REPLY), acceptor_(acceptor), id_(id), accept_(accept) { }

    unsigned int type_;
    unsigned int acceptor_;
    ProposalID id_;
    AcceptingID accept_;
};
#pragma pack(pop)

void PaxosImpl::OnAccept(const AcceptMsg * msg)
{
    DEBUG(logger_, "proposal id: %llu, promised proposal id: %llu\n", msg->id_, promised_proposal_id_);

    if (msg->id_ > max_proposal_id_)
        max_proposal_id_ = msg->id_;

    if (msg->id_ >= promised_proposal_id_)
    {
        //std::string d = DumpHex(msg->values_, msg->len_);
        //DEBUG(logger_, "len = %u, d = %s\n", msg->len_, d.c_str());

        std::map<InstanceID, Value> values;
        ExtractInstanceValues(logger_, msg->values_, msg->len_, &values);

        std::string dmp;
        for (std::map<InstanceID, Value>::iterator it = values.begin();
                it != values.end();
                ++it)
            // Values to be accepted may not be the same as the values already committed
            // as in this case the accept request can still be rejected by others.
            if (committed_values_.find(it->first) == committed_values_.end())
            {
                if (dmp.length()) dmp += ", ";
                dmp += "[" + ToStr(it->first) + "] = " + Debug(it->second, sm_);
                if (accepted_values_.find(it->first) != accepted_values_.end())
                    dmp += " replacing " + Debug(accepted_values_[it->first], sm_);

                accepted_values_[it->first] = AcceptedValue(msg->id_, it->second);
            }
        DEBUG(logger_, "accept values from %u: %s\n", msg->proposer_, dmp.c_str());

        AcceptReplyMsg reply(index_, msg->id_, msg->accept_);
        std::string r((const char *)&reply, sizeof reply);

        DEBUG(logger_, "reply accept to %u for %llu\n", msg->proposer_, msg->accept_);

        net_->SendMessageUDP(nodes_[msg->proposer_].ip_, nodes_[msg->proposer_].port_, r);
    }
    else
    {
        RejectMsg reject(max_proposal_id_);
        std::string r((const char *)&reject, sizeof reject);
        net_->SendMessageUDP(nodes_[msg->proposer_].ip_, nodes_[msg->proposer_].port_, r);
    }
}

void PaxosImpl::OnAcceptReply(const AcceptReplyMsg * msg)
{
    if (msg->id_ != proposal_id_) return;

    if (accepting_values_.find(msg->accept_) == accepting_values_.end()) return;

    AcceptingValues * accept = accepting_values_[msg->accept_];

    ASSERT(logger_, nodes_.find(msg->acceptor_) != nodes_.end());
    accept->accepted_.insert(msg->acceptor_);
    if (accept->accepted_.size() >= nodes_.size() / 2 + 1)
    {
        CommittingValues * commit = new CommittingValues(++committing_id_, proposal_id_, accept->values_);
        committing_values_.insert(std::make_pair(committing_id_, commit));
        commit->retry_timeout_ = new CommitRetryTimeout(this, commit);
        Commit(commit);

        accept->retry_timeout_->Cancel();
        accepting_values_.erase(msg->accept_);
        delete accept;
    }
}

static const unsigned int MSG_COMMIT = 5;
#pragma pack(push, 1)
struct CommitMsg
{
    CommitMsg(unsigned int committer, CommittingID commit, ProposalID id, unsigned int len)
        : type_(MSG_COMMIT), committer_(committer), commit_(commit), id_(id), len_(len) { }

    unsigned int type_;
    unsigned int committer_;
    CommittingID commit_;
    ProposalID id_;

    unsigned int len_;
    char values_[0];
};
#pragma pack(pop)

void PaxosImpl::Commit(CommittingValues * commit)
{
    std::string dmp;
    for (std::map<InstanceID, Value>::iterator it = commit->values_.begin();
            it != commit->values_.end();
            ++it)
    {
        if (dmp.length()) dmp += ", ";
        dmp += "[" + ToStr(it->first) + "] = " + Debug(it->second, sm_);
    }
    std::string rdmp;
    for (std::set<unsigned int>::iterator it = commit->replied_.begin();
            it != commit->replied_.end();
            ++it)
    {
        if (rdmp.length()) rdmp += ", ";
        rdmp += ToStr(*it);
    }
    DEBUG(logger_, "broadcast commit: %s (replied = %s)\n", dmp.c_str(), rdmp.length() ? rdmp.c_str() : "None");

    unsigned int len = CalcInstanceValuesLength(&commit->values_);
    unsigned int size = sizeof (CommitMsg) + len;

    CommitMsg * msg = new (new char[size]) CommitMsg(index_, commit->id_, commit->proposal_id_, len);
    FillInstanceValues(msg->values_, &commit->values_);
    std::string m((const char *)msg, size);
    delete [] (char *)msg;

    for (NodeInfoMap::iterator it = nodes_.begin(); it != nodes_.end(); ++it)
        if (commit->replied_.find(it->first) == commit->replied_.end())
            net_->SendMessageTCP(it->second.ip_, it->second.port_, m);

    timer_->AddTimeout(commit->retry_timeout_, clock_->Now() + config_.commit_retry_timeout_);
}

static const unsigned int MSG_COMMIT_REPLY = 6;
#pragma pack(push, 1)
struct CommitReplyMsg
{
    CommitReplyMsg(unsigned int learner, CommittingID commit)
        : type_(MSG_COMMIT_REPLY), learner_(learner), commit_(commit) { }

    unsigned int type_;
    unsigned int learner_;
    CommittingID commit_;
};
#pragma pack(pop)

void PaxosImpl::OnCommit(const CommitMsg * msg)
{
    std::map<InstanceID, Value> values;
    ExtractInstanceValues(logger_, msg->values_, msg->len_, &values);

    AcceptingValues * accept = NULL;

    for (std::map<InstanceID, Value>::iterator it = values.begin();
            it != values.end();
            ++it)
    {
        if (accepted_values_.find(it->first) != accepted_values_.end())
            accepted_values_.erase(it->first);

        if (committed_values_.find(it->first) != committed_values_.end())
            ASSERT(logger_, it->second == committed_values_[it->first].value_);
        else
        {
            if (it->second.proposer_ == index_ && !it->second.noop_)
                ASSERT(logger_, uncommitted_proposed_values_.find(it->second.value_id_) != uncommitted_proposed_values_.end());

            committed_values_.insert(std::make_pair(it->first, AcceptedValue(msg->id_, it->second)));

            uncommitted_instance_ids_.Remove(it->first);
        }

        // We can't propose to the committed instance id as
        // it's committed and it may not be committed again,
        // while we need to check the unproposed instance ids as
        // the id may already used and in this case, it will
        // be re-proposed with another id
        if (unproposed_instance_ids_.Contains(it->first))
            // This may still cause trouble, if the id removed here,
            // it can't be used in receving pre-accepted values.
            unproposed_instance_ids_.Remove(it->first);

        if (it->second.proposer_ == index_
            && uncommitted_proposed_values_.find(it->second.value_id_) != uncommitted_proposed_values_.end())
        {
            // Callback called in OnCommit rather than Commit
            // because value may not be committed by its initial
            // proposer
            uncommitted_proposed_values_[it->second.value_id_].cb_->Run();
            uncommitted_proposed_values_.erase(it->second.value_id_);
        }

        if (initial_proposals_.find(it->first) != initial_proposals_.end())
        {
            ValueID value_id = initial_proposals_[it->first];
            if (it->second.proposer_ != index_ || it->second.value_id_ != value_id)
            {
                ASSERT(logger_, uncommitted_proposed_values_.find(value_id) != uncommitted_proposed_values_.end());
                if (!prepare_retry_timeout_)
                {
                    if (!accept)
                    {
                        accept = new AcceptingValues(logger_, ++accepting_id_);
                        accepting_values_.insert(std::make_pair(accepting_id_, accept));
                    }

                    // The on-going committed values may include this new id,
                    // but this should still work since this accept must be
                    // outdated and will be rejected
                    InstanceID instance_id = unproposed_instance_ids_.Next();
                    ASSERT(logger_, initial_proposals_.insert(std::make_pair(instance_id, value_id)).second);

                    const ProposedValue * proposed = &uncommitted_proposed_values_[value_id];
                    accept->AddValue(instance_id, !proposed->membership_change_
                        ? Value(index_, value_id, proposed->value_)
                        : Value(index_, value_id,  *proposed->membership_change_));
                }
                else
                    newly_proposed_values_.insert(value_id);
            }
            initial_proposals_.erase(it->first);
        }
    }

    CommitReplyMsg reply(index_, msg->commit_);
    std::string r((const char *)&reply, sizeof reply);

    DEBUG(logger_, "reply commit to %u for %llu\n", msg->committer_, msg->commit_);
    net_->SendMessageTCP(nodes_[msg->committer_].ip_, nodes_[msg->committer_].port_, r);

    if (accept)
    {
        accept->retry_timeout_ = new AcceptRetryTimeout(this, accept, config_.accept_retry_count_);
        Accept(accept);
    }

    std::string dmp;
    while (committed_values_.find(next_id_to_apply_) != committed_values_.end())
    {
        const AcceptedValue & value = committed_values_[next_id_to_apply_++];
        if (dmp.length()) dmp += ", ";
        dmp += "[" + ToStr(next_id_to_apply_ - 1) + "] = " + Debug(value, sm_);
        if (value.value_.noop_)
            continue;
#if 0
        else if (value.value_.membership_change_)
        {
            MembershipChange * change = value.value_.membership_change_;
            if (change->node_)
            {
                ASSERT(logger_, nodes_.insert(std::make_pair(change->id_, *change->node_)).second);
            }
            else
            {
                ASSERT(logger_, nodes_.find(change->id_) != nodes_.end());
                nodes_.erase(change->id_);

                // If this node is removed, the node can still be the proposer
                // and we can still ensure the success
            }

            if (prepare_retry_timeout_)
            {
                prepare_retry_timeout_->Cancel();
                RestartPrepare();
            }
            else
                AcceptRejected();
        }
#endif
        else
            sm_->Execute(value.value_.value_);
    }
    if (dmp.length())
        DEBUG(logger_, "execute: %s\n", dmp.c_str());
}

void PaxosImpl::OnCommitReply(const CommitReplyMsg * msg)
{
    if (committing_values_.find(msg->commit_) == committing_values_.end()) return;

    DEBUG(logger_, "commit replied from %u for %llu\n", msg->learner_, msg->commit_);

    CommittingValues * commit = committing_values_[msg->commit_];

    commit->replied_.insert(msg->learner_);

    if (commit->replied_.size() == nodes_.size())
    {
        commit->retry_timeout_->Cancel();
        committing_values_.erase(msg->commit_);
        delete commit;
    }
}

void PaxosImpl::Loop()
{
    SetThreadName(thread_prefix_ + "-paxos");

    StartPrepare();

    while (!AtomicGet(&stop_))
    {
        timer_->Process(clock_->Now());

        std::string msg;
        while (received_.Get(&msg))
        {
            unsigned int type = GetMsgType(msg.c_str());
            if (type == MSG_PREPARE)
                OnPrepare((const PrepareMsg *)msg.c_str());
            else if (type == MSG_PREPARE_REPLY)
                OnPrepareReply((const PrepareReplyMsg *)msg.c_str());
            else if (type == MSG_REJECT)
                OnReject((const RejectMsg *)msg.c_str());
            else if (type == MSG_ACCEPT)
                OnAccept((const AcceptMsg *)msg.c_str());
            else if (type == MSG_ACCEPT_REPLY)
                OnAcceptReply((const AcceptReplyMsg *)msg.c_str());
            else if (type == MSG_COMMIT)
                OnCommit((const CommitMsg *)msg.c_str());
            else if (type == MSG_COMMIT_REPLY)
                OnCommitReply((const CommitReplyMsg *)msg.c_str());
            else
                ASSERT(logger_, false);
        }

        ProposedValue proposed;
        while (proposed_.Get(&proposed))
            Propose(proposed);

        usleep(100);
    }

    ASSERT(logger_, received_.Empty());
    ASSERT(logger_, proposed_.Empty());
    ASSERT(logger_, uncommitted_proposed_values_.empty());
    ASSERT(logger_, !prepare_retry_timeout_);
    ASSERT(logger_, prepare_promised_.empty());
    ASSERT(logger_, initial_proposals_.empty());
    ASSERT(logger_, newly_proposed_values_.empty());
    ASSERT(logger_, pre_accepted_values_.empty());
    ASSERT(logger_, accepting_values_.empty());
    ASSERT(logger_, accepted_values_.empty());
    ASSERT(logger_, committing_values_.empty());

    std::string dmp;
    for (std::map<InstanceID, AcceptedValue>::iterator it = committed_values_.begin();
            it != committed_values_.end();
            ++it)
    {
        if (dmp.length()) dmp += ", ";
        dmp += Debug(it->second, sm_);
    }

    DEBUG(logger_, "final committed values: %s (%u in total)\n", dmp.c_str(), committed_values_.size());

    UnsetThreadName();
}

void * RunPaxos(void * arg)
{
    ((PaxosImpl *)arg)->Loop();
    return NULL;
}

void NetWork::OnReceiveMessage(const char * msg, unsigned int len)
{
    paxos_->OnReceiveMessage(std::string(msg, len));
}

Paxos::Paxos(Logger * logger, const std::string & thread_prefix,
                Clock * clock, Timer * timer, Rand * rand,
                const NodeInfoMap & nodes, unsigned int index,
                NetWork * net, StateMachine * sm,
                const Config & config,
                unsigned int * whole_system_reference_count_for_debugging)
    : impl_(new PaxosImpl(logger, thread_prefix, clock, timer, rand,
                            nodes, index, net, sm, config,
                            whole_system_reference_count_for_debugging)) { }

Paxos::~Paxos()
{
    delete impl_;
}

void Paxos::Propose(const std::string & value, Callback * cb)
{
    impl_->Propose(value, cb);
}

#if 0
void Paxos::AddMember(unsigned int id, const NodeInfo & node, Callback * cb)
{
    impl_->AddMember(id, node, cb);
}

void Paxos::DelMember(unsigned int id, Callback * cb)
{
    impl_->DelMember(id, cb);
}
#endif

}

#ifdef UNITTEST

static void Test(const paxos::Value & v)
{
    unsigned int size = paxos::CalcValueLength(&v);
    printf("size = %u\n", size);
    char * buf = new char[size];
    unsigned int cur = 0;
    paxos::FillValue(buf, cur, &v);
    assert(cur == size);

    cur = 0;
    assert(paxos::ExtractValue(buf, cur) == v);
    assert(cur == size);

    delete [] buf;
}

int main()
{
    Test(paxos::Value(1, 2));
    Test(paxos::Value(1, 2, std::string("123")));
    return 0;
}

#endif
