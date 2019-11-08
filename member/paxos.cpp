#include <stdarg.h>
#include <time.h>
#include <sys/syscall.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <vector>
#include <set>
#include <algorithm>

#include "paxos.h"

// Debug value format:
//   no-op:      [instance-id] = <proposal-id>(proposer:value-id)-
//   normal:     [instance-id] = <proposal-id>(proposer:value-id)+value{cb}
//   member:     [instance-id] = <proposal-id>(proposer:value-id)midtypes{cb}

static void FormatCurrentTime(TimeStamp now, char * time)
{
    time_t tm = (time_t)(now / 1000);
    strftime(time, 20, "%Y-%m-%d %H:%M:%S", localtime(&tm));
    time[19] = '.';
    snprintf(time + 20, 4, "%03llu", now % 1000);
}

void Logger::Log(unsigned int level, Thread * thread,
                    const char * file, int line, const char * function, const char * format, ...)
{
    thread->RandomFailure();

    if (level < level_) return;

    lock_->Lock(thread);

    const std::string tid = thread->GetID();

    char time[64];
    FormatCurrentTime(clock_->Now(thread), time);

    static const char * level_desc [] = {
        "TRACE", "DEBUG", "INFO", "NOTICE", "WARNING", "ERROR", "CRITICAL"
    };
    printf("[%s]\t[%s]\t", time, level_desc[level]);
    printf("[%s:%s]\t", thread->GetName().c_str(), tid.c_str());
    printf("[%s:%d]\t[%s]\t", file, line, function);

    va_list args;
    va_start(args, format);
    vprintf(format, args);
    va_end(args);

    fflush(stdout);

    lock_->Unlock();
}

namespace paxos
{

enum MembershipChangeType
{
    ADD_LEARNER,
    LEARNER_TO_PROPOSER,
    PROPOSER_TO_ACCEPTOR,
    DEL_LEARNER,
    PROPOSER_TO_LEARNER,
    ACCEPTOR_TO_PROPOSER
};

struct MembershipChange
{
    MembershipChange(NodeID node, MembershipChangeType type)
        : node_(node), type_(type) { }
    MembershipChange(const MembershipChange & m)
        : node_(m.node_), type_(m.type_) { }

    bool operator==(const MembershipChange & m) const
    {
        return node_ == m.node_ && type_ == m.type_;
    }

    NodeID node_;
    MembershipChangeType type_;
};

std::string Debug(const std::vector<MembershipChange> & ms)
{
    std::string dmp;
    const char * types = "LPAlpa";
    NodeID id = (NodeID)-1;
    for (std::vector<MembershipChange>::const_iterator it = ms.begin();
            it != ms.end();
            ++it)
    {
        dmp += it->node_ == id ? "" : ToStr(it->node_) + types[it->type_];
        id = it->node_;
    }
    return dmp;
}

struct ProposedValue
{
    ProposedValue() : membership_changes_(NULL) { }

    ProposedValue(const std::string & value, const std::string & cb)
        : membership_changes_(NULL), value_(value), cb_(cb) { }
    ProposedValue(const std::vector<MembershipChange> & changes, const std::string & cb)
        : membership_changes_(new std::vector<MembershipChange>(changes)), cb_(cb) { }
    ProposedValue(const ProposedValue & p)
        : membership_changes_(p.membership_changes_ ? new std::vector<MembershipChange>(*p.membership_changes_) : NULL),
            value_(p.value_), cb_(p.cb_) { }

    ProposedValue & operator=(const ProposedValue & p)
    {
        membership_changes_ = p.membership_changes_ ? new std::vector<MembershipChange>(*p.membership_changes_) : NULL;
        value_ = p.value_;
        cb_ = p.cb_;
        return *this;
    }

    ~ProposedValue()
    {
        if (membership_changes_) delete membership_changes_;
    }

    std::vector<MembershipChange> * membership_changes_;

    std::string value_;
    std::string cb_;
};

static std::string Debug(const ProposedValue & v)
{
    return (v.membership_changes_ ? 'm' + Debug(*v.membership_changes_) : '+' + v.value_) + '{' + v.cb_ + '}';
}

typedef unsigned long long ValueID;

struct Value
{
    Value() : membership_changes_(NULL) { }
    Value(NodeID proposer, ValueID value_id)
        : proposer_(proposer), value_id_(value_id), noop_(true), membership_changes_(NULL) { }
    Value(NodeID proposer, ValueID value_id, const ProposedValue & proposed)
        : proposer_(proposer), value_id_(value_id), noop_(false),
            membership_changes_(proposed.membership_changes_ ? new std::vector<MembershipChange>(*proposed.membership_changes_) : NULL),
            value_(proposed.membership_changes_ ? std::string() : proposed.value_),
            cb_(proposed.cb_) { }
    Value(const Value & v)
        : proposer_(v.proposer_), value_id_(v.value_id_), noop_(v.noop_),
            membership_changes_(v.membership_changes_ ? new std::vector<MembershipChange>(*v.membership_changes_) : NULL),
            value_(v.value_),
            cb_(v.cb_) { }

    ~Value()
    {
        if (membership_changes_) delete membership_changes_;
    }

    bool operator==(const Value & v) const
    {
        return proposer_ == v.proposer_ && value_id_ == v.value_id_
            && noop_ == v.noop_
            && (membership_changes_ && v.membership_changes_ ? *membership_changes_ == *v.membership_changes_ : membership_changes_ == v.membership_changes_)
            && value_ == v.value_
            && cb_ == v.cb_;
    }

    // The initial proposer
    NodeID proposer_;
    ValueID value_id_;
    // No-op is required to ensure the order
    bool noop_;
    std::vector<MembershipChange> * membership_changes_;
    std::string value_;
    std::string cb_;
};

static std::string Debug(const Value & v)
{
    return '(' + ToStr(v.proposer_) + ':' + ToStr(v.value_id_) + ')' +
            (v.noop_ ? "-" : (v.membership_changes_ ? 'm' + Debug(*v.membership_changes_) : '+' + v.value_) + '{' + v.cb_ + '}');
}

typedef unsigned long long InstanceID;

struct AvailableInstanceIDs
{
public:
    AvailableInstanceIDs()
    {
        ids_.insert(std::make_pair(0, (InstanceID)-1));
    }

    AvailableInstanceIDs & operator=(const AvailableInstanceIDs & ids)
    {
        ids_ = ids.ids_;
        return *this;
    }

    InstanceID Next()
    {
        InstanceID a = ids_.begin()->first;
        Remove(a);
        return a;
    }

    bool Contain(InstanceID id)
    {
        std::set<std::pair<InstanceID, InstanceID> >::iterator after = ids_.upper_bound(std::make_pair(id, (InstanceID)-1));
        if (after == ids_.begin()) return false;
        std::set<std::pair<InstanceID, InstanceID> >::iterator cur = --after;
        return cur->first <= id && cur->second > id;
    }

    void Remove(InstanceID id)
    {
        std::set<std::pair<InstanceID, InstanceID> >::iterator after = ids_.upper_bound(std::make_pair(id, (InstanceID)-1));
        std::set<std::pair<InstanceID, InstanceID> >::iterator cur = --after;

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

    std::set<std::pair<InstanceID, InstanceID> > ids_;
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

static void ExtractAvailableInstanceIDs(Logger * logger, Thread * thread,
                                        const char * buf, unsigned int len, AvailableInstanceIDs * ids)
{
    ids->ids_.clear();

    unsigned int cur = 0;
    while (cur != len)
    {
        InstanceID a = *(const InstanceID *)(buf + cur);
        InstanceID b = *(const InstanceID *)(buf + cur + sizeof (InstanceID));
        cur += 2 * sizeof (InstanceID);

        ASSERT(logger, thread, ids->ids_.insert(std::make_pair(a, b)).second);
    }
}

typedef unsigned long long ProposalID;

struct ProposalValue
{
    ProposalValue() { }
    ProposalValue(ProposalID proposal_id, const Value & value)
        : proposal_id_(proposal_id), value_(value) { }
    ProposalValue(const ProposalValue & v)
        : proposal_id_(v.proposal_id_), value_(v.value_) { }

    bool operator==(const ProposalValue & v) const
    {
        return proposal_id_ == v.proposal_id_ && value_ == v.value_;
    }

    ProposalID proposal_id_;
    Value value_;
};

static std::string Debug(const ProposalValue & v)
{
    return "<" + ToStr(v.proposal_id_) + ">" + Debug(v.value_);
}

static std::string Debug(const std::map<InstanceID, ProposalValue> & values,
                            const std::map<InstanceID, ProposalValue> * rep = NULL)
{
    std::string dmp;
    for (std::map<InstanceID, ProposalValue>::const_iterator it = values.begin();
            it != values.end();
            ++it)
    {
        if (dmp.length()) dmp += ", ";
        dmp += "[" + ToStr(it->first) + "] = " + Debug(it->second);
        if (rep && rep->find(it->first) != rep->end())
            dmp += " replacing " + Debug(rep->find(it->first)->second);
    }
    return dmp;
}

static unsigned int CalcValueLength(const Value & val)
{
    return sizeof (unsigned int) + sizeof (ValueID) + sizeof (bool) + (!val.noop_
        ? sizeof (bool) + (val.membership_changes_
            ? sizeof (unsigned int) + (sizeof (NodeID) + sizeof (MembershipChangeType)) * val.membership_changes_->size()
            : sizeof (unsigned int) + val.value_.size()) + sizeof (unsigned int) + val.cb_.size()
        : 0);
}

static void FillValue(char * buf, unsigned int & cur, const Value & val)
{
    *(unsigned int *)(buf + cur) = val.proposer_;
    *(ValueID *)(buf + (cur += sizeof (unsigned int))) = val.value_id_;
    *(bool *)(buf + (cur += sizeof (ValueID))) = val.noop_;
    if (!val.noop_)
    {
        *(bool *)(buf + (cur += sizeof (bool))) = val.membership_changes_;
        if (val.membership_changes_)
        {
            *(unsigned int *)(buf + (cur += sizeof (bool))) = val.membership_changes_->size();
            cur += sizeof (unsigned int);
            for (std::vector<MembershipChange>::const_iterator it = val.membership_changes_->begin();
                    it != val.membership_changes_->end();
                    ++it)
            {
                *(NodeID *)(buf + cur) = it->node_;
                *(MembershipChangeType *)(buf + (cur += sizeof (NodeID))) = it->type_;
                cur += sizeof (MembershipChangeType);
            }
        }
        else
        {
            *(unsigned int *)(buf + (cur += sizeof (bool))) = val.value_.size();
            memcpy(buf + (cur += sizeof (unsigned int)), val.value_.c_str(), val.value_.size());
            cur += val.value_.size();
        }
        *(unsigned int *)(buf + cur) = val.cb_.size();
        memcpy(buf + (cur += sizeof (unsigned int)), val.cb_.c_str(), val.cb_.size());
        cur += val.cb_.size();
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
        ProposedValue proposed;
        bool membership = *(const bool *)(buf + (cur += sizeof (bool)));
        if (membership)
        {
            proposed.membership_changes_ = new std::vector<MembershipChange>;
            unsigned int len = *(const unsigned int *)(buf + (cur += sizeof (bool)));
            cur += sizeof (unsigned int);
            for (unsigned int i = 0; i < len; ++i)
            {
                NodeID node = *(const NodeID *)(buf + cur);
                MembershipChangeType type = *(const MembershipChangeType *)(buf + (cur += sizeof (NodeID)));
                cur += sizeof (MembershipChangeType);
                proposed.membership_changes_->push_back(MembershipChange(node, type));
            }
        }
        else
        {
            unsigned int len = *(const unsigned int *)(buf + (cur += sizeof (bool)));
            proposed.value_ = std::string(buf + (cur += sizeof (unsigned int)), len);
            cur += len;
        }
        unsigned int len = *(const unsigned int *)(buf + cur);
        proposed.cb_ = std::string(buf + (cur += sizeof (unsigned int)), len);
        cur += len;
        return Value(proposer, value_id, proposed);
    }
    else
    {
        cur += sizeof (bool);
        return Value(proposer, value_id);
    }
}

static unsigned int CalcProposalValuesLength(const std::map<InstanceID, ProposalValue> & values)
{
    unsigned int len = (sizeof (InstanceID) + sizeof (ProposalID)) * values.size();
    for (std::map<InstanceID, ProposalValue>::const_iterator it = values.begin();
            it != values.end();
            ++it)
        len += CalcValueLength(it->second.value_);
    return len;
}

static void FillProposalValues(char * buf, const std::map<InstanceID, ProposalValue> & values)
{
    unsigned int cur = 0;
    for (std::map<InstanceID, ProposalValue>::const_iterator it = values.begin();
            it != values.end();
            ++it)
    {
        *(InstanceID *)(buf + cur) = it->first;
        *(ProposalID *)(buf + (cur += sizeof (InstanceID))) = it->second.proposal_id_;
        FillValue(buf, cur += sizeof (ProposalID), it->second.value_);
    }
}

static void ExtractProposalValues(Logger * logger, Thread * thread,
                                    const char * buf, unsigned int len, std::map<InstanceID, ProposalValue> * values)
{
    unsigned int cur = 0;
    while (cur != len)
    {
        InstanceID id = *(const InstanceID *)(buf + cur);
        ProposalID proposal_id = *(const ProposalID *)(buf + (cur += sizeof (InstanceID)));
        
        ASSERT(logger, thread,
                values->insert(std::make_pair(id, ProposalValue(proposal_id, ExtractValue(buf, cur += sizeof (ProposalID))))).second);
    }
}

typedef unsigned long long AcceptingID;
class AcceptRetryTimeout;

struct AcceptingValues
{
    AcceptingValues(AcceptingID id,
                    const std::map<InstanceID, ProposalValue> & values)
        : id_(id), values_(values), retry_timeout_(NULL) { }

    AcceptingID id_;
    std::map<InstanceID, ProposalValue> values_;

    std::set<unsigned int> accepted_;

    AcceptRetryTimeout * retry_timeout_;
};

typedef unsigned long long LearningID;
class LearnRetryTimeout;

struct LearningValues
{
    LearningValues(LearningID id,
                    const std::map<InstanceID, ProposalValue> & values)
        : id_(id), values_(values), retry_timeout_(NULL) { }

    LearningID id_;
    std::map<InstanceID, ProposalValue> values_;

    std::set<unsigned int> learned_;

    LearnRetryTimeout * retry_timeout_;
};

struct PrepareMsg;
struct PrepareReplyMsg;
struct RejectMsg;
struct AcceptMsg;
struct AcceptReplyMsg;
struct LearnMsg;
struct LearnReplyMsg;

class Learner
{
public:
    Learner(NodeImpl * node, StateMachine * sm) : node_(node), sm_(sm), next_id_to_apply_(0) { }

    void OnLearn(const LearnMsg * msg);

    const std::map<InstanceID, ProposalValue> & LearnedValues() const { return learned_values_; }

private:
    bool Apply(const Value & value, std::string * result);

private:
    NodeImpl * node_;

    std::map<InstanceID, ProposalValue> learned_values_;

    StateMachine * sm_;
    InstanceID next_id_to_apply_;
};

class PrepareDelay;
class PrepareRetryTimeout;

class Proposer
{
public:
    Proposer(NodeImpl * node);
    ~Proposer();

    void Propose(const ProposedValue & proposed);

    void OnPrepareReply(const PrepareReplyMsg * msg);
    void OnReject(const RejectMsg * msg);
    void OnAcceptReply(const AcceptReplyMsg * msg);
    void OnLearnReply(const LearnReplyMsg * msg);

    void OnLearn(const std::map<InstanceID, ProposalValue> & values);

    void LearnersChanged();
    void AcceptorsChanged(bool add, NodeID node);

private:
    void StartPrepare();
    void UpdateProposalID();

    friend class PrepareRetryTimeout;
    void RestartPrepare();
    friend class PrepareDelay;
    void DelaiedPrepare();

    void Prepare();

    void UpdateByPreAcceptedValues(const std::map<InstanceID, ProposalValue> & values);

    friend class AcceptRetryTimeout;
    void Accept(AcceptingValues * accept);
    void AcceptRejected();

    friend class LearnRetryTimeout;
    void Learn(LearningValues * learn);

private:
    NodeImpl * node_;

    ValueID value_id_;
    std::map<ValueID, ProposedValue> unlearned_proposed_values_;

    AvailableInstanceIDs unlearned_instance_ids_;
    AvailableInstanceIDs preparing_instance_ids_;
    AvailableInstanceIDs unproposed_instance_ids_;

    ProposalID max_proposal_id_;
    unsigned long long proposal_count_;
    ProposalID proposal_id_;

    PrepareRetryTimeout * prepare_retry_timeout_;
    PrepareDelay * prepare_delay_;
    std::set<NodeID> prepare_promised_;

    std::map<InstanceID, ValueID> initial_proposals_;
    std::set<ValueID> newly_proposed_values_;

    std::map<InstanceID, ProposalValue> pre_accepted_values_;

    AcceptingID accepting_id_;
    std::map<AcceptingID, AcceptingValues *> accepting_values_;

    LearningID learning_id_;
    std::map<LearningID, LearningValues *> learning_values_;
    std::map<LearningID, std::set<NodeID> > learning_values_for_acceptors_;
};

class Acceptor
{
public:
    Acceptor(NodeImpl * node) : node_(node), max_proposal_id_(0), promised_proposal_id_(0) { }

    void OnPrepare(const PrepareMsg * msg);
    void OnAccept(const AcceptMsg * msg);

    void OnLearn(const std::map<InstanceID, ProposalValue> & values);

private:
    void FilterAcceptedValues(const AvailableInstanceIDs * ids,
                                std::map<InstanceID, ProposalValue> * values) const;

private:
    NodeImpl * node_;

    ProposalID max_proposal_id_;
    ProposalID promised_proposal_id_;
    std::map<InstanceID, ProposalValue> accepted_values_;
};

typedef unsigned int Version;

static unsigned int GetMsgType(const char * msg)
{
    return *(unsigned int *)msg;
}

static const unsigned int MSG_PREPARE = 0;
static const unsigned int MSG_PREPARE_REPLY = 1;
static const unsigned int MSG_REJECT = 2;
static const unsigned int MSG_ACCEPT = 3;
static const unsigned int MSG_ACCEPT_REPLY = 4;
static const unsigned int MSG_LEARN = 5;
static const unsigned int MSG_LEARN_REPLY = 6;

struct NodeImpl
{
    NodeImpl(Thread * thread, NodeID id, NodeID first,
                Logger * logger, Clock * clock, Timer * timer, Rand * rand,
                Callback * cb, NetWork * net, StateMachine * sm,
                const Config & config);
    ~NodeImpl();

    void Init(const std::string & thread_prefix, Thread * thread);
    void Fini(const Thread * thread);

    void OnReceive(const std::string & msg, const Thread * thread)
    {
        received_.Put(msg, thread);
    }

    void Propose(const std::string & value, const std::string & cb, const Thread * thread)
    {
        proposed_.Put(ProposedValue(value, cb), thread);
    }

    void AddLearner(NodeID id, const std::string & cb, const Thread * thread)
    {
        std::vector<MembershipChange> changes;
        changes.push_back(MembershipChange(id, ADD_LEARNER));
        proposed_.Put(ProposedValue(changes, cb), thread);
    }

    void AddProposer(NodeID id, const std::string & cb, const Thread * thread)
    {
        std::vector<MembershipChange> changes;
        changes.push_back(MembershipChange(id, ADD_LEARNER));
        changes.push_back(MembershipChange(id, LEARNER_TO_PROPOSER));
        proposed_.Put(ProposedValue(changes, cb), thread);
    }

    void AddAcceptor(NodeID id, const std::string & cb, const Thread * thread)
    {
        std::vector<MembershipChange> changes;
        changes.push_back(MembershipChange(id, ADD_LEARNER));
        changes.push_back(MembershipChange(id, LEARNER_TO_PROPOSER));
        changes.push_back(MembershipChange(id, PROPOSER_TO_ACCEPTOR));
        proposed_.Put(ProposedValue(changes, cb), thread);
    }

    void LearnerToProposer(NodeID id, const std::string & cb, const Thread * thread)
    {
        std::vector<MembershipChange> changes;
        changes.push_back(MembershipChange(id, LEARNER_TO_PROPOSER));
        proposed_.Put(ProposedValue(changes, cb), thread);
    }

    void LearnerToAcceptor(NodeID id, const std::string & cb, const Thread * thread)
    {
        std::vector<MembershipChange> changes;
        changes.push_back(MembershipChange(id, LEARNER_TO_PROPOSER));
        changes.push_back(MembershipChange(id, PROPOSER_TO_ACCEPTOR));
        proposed_.Put(ProposedValue(changes, cb), thread);
    }

    void ProposerToAcceptor(NodeID id, const std::string & cb, const Thread * thread)
    {
        std::vector<MembershipChange> changes;
        changes.push_back(MembershipChange(id, PROPOSER_TO_ACCEPTOR));
        proposed_.Put(ProposedValue(changes, cb), thread);
    }

    void DelLearner(NodeID id, const std::string & cb, const Thread * thread)
    {
        std::vector<MembershipChange> changes;
        changes.push_back(MembershipChange(id, DEL_LEARNER));
        proposed_.Put(ProposedValue(changes, cb), thread);
    }

    void DelProposer(NodeID id, const std::string & cb, const Thread * thread)
    {
        std::vector<MembershipChange> changes;
        changes.push_back(MembershipChange(id, PROPOSER_TO_LEARNER));
        changes.push_back(MembershipChange(id, DEL_LEARNER));
        proposed_.Put(ProposedValue(changes, cb), thread);
    }

    void DelAcceptor(NodeID id, const std::string & cb, const Thread * thread)
    {
        std::vector<MembershipChange> changes;
        changes.push_back(MembershipChange(id, ACCEPTOR_TO_PROPOSER));
        changes.push_back(MembershipChange(id, PROPOSER_TO_LEARNER));
        changes.push_back(MembershipChange(id, DEL_LEARNER));
        proposed_.Put(ProposedValue(changes, cb), thread);
    }

    void ProposerToLearner(NodeID id, const std::string & cb, const Thread * thread)
    {
        std::vector<MembershipChange> changes;
        changes.push_back(MembershipChange(id, PROPOSER_TO_LEARNER));
        proposed_.Put(ProposedValue(changes, cb), thread);
    }

    void AcceptorToLearner(NodeID id, const std::string & cb, const Thread * thread)
    {
        std::vector<MembershipChange> changes;
        changes.push_back(MembershipChange(id, ACCEPTOR_TO_PROPOSER));
        changes.push_back(MembershipChange(id, PROPOSER_TO_LEARNER));
        proposed_.Put(ProposedValue(changes, cb), thread);
    }

    void AcceptorToProposer(NodeID id, const std::string & cb, const Thread * thread)
    {
        std::vector<MembershipChange> changes;
        changes.push_back(MembershipChange(id, ACCEPTOR_TO_PROPOSER));
        proposed_.Put(ProposedValue(changes, cb), thread);
    }

    void Loop()
    {
        learners_.insert(first_);
        proposers_.insert(first_);
        acceptors_.insert(first_);

        if (first_ == id_)
        {
            proposer_ = new Proposer(this);
            acceptor_ = new Acceptor(this);
        }

        while (!stop_->Get(thread_))
        {
            timer_->Process(clock_->Now(thread_));

            std::string msg;
            while (received_.Get(&msg, thread_))
            {
                unsigned int type = GetMsgType(msg.c_str());
                if (type == MSG_PREPARE)
                {
                    if (acceptor_)
                        acceptor_->OnPrepare((const PrepareMsg *)msg.c_str());
                }
                else if (type == MSG_PREPARE_REPLY)
                {
                    if (proposer_)
                        proposer_->OnPrepareReply((const PrepareReplyMsg *)msg.c_str());
                }
                else if (type == MSG_REJECT)
                {
                    if (proposer_)
                        proposer_->OnReject((const RejectMsg *)msg.c_str());
                }
                else if (type == MSG_ACCEPT)
                {
                    if (acceptor_)
                        acceptor_->OnAccept((const AcceptMsg *)msg.c_str());
                }
                else if (type == MSG_ACCEPT_REPLY)
                {
                    if (proposer_)
                        proposer_->OnAcceptReply((const AcceptReplyMsg *)msg.c_str());
                }
                else if (type == MSG_LEARN)
                    learner_.OnLearn((const LearnMsg *)msg.c_str());
                else if (type == MSG_LEARN_REPLY)
                {
                    if (proposer_)
                        proposer_->OnLearnReply((const LearnReplyMsg *)msg.c_str());
                }
                else
                    ASSERT(logger_, thread_, false);
            }

            ProposedValue proposed;
            while (proposed_.Get(&proposed, thread_))
            {
                if (!proposer_)
                    cb_->Unproposable(thread_, proposed.cb_);
                else
                    proposer_->Propose(proposed);
            }
        }

        if (proposer_)
        {
            delete proposer_;
            if (acceptor_) delete acceptor_;
        }

        thread_->USleep(1000);
    }

    void ChangeMemberships(const std::vector<MembershipChange> & actions);

    Thread * thread;
    Thread * thread_;

    AtomicBool * stop_;

    // To ensure ids generated by the proposer valid
    bool proposered_;

    NodeID id_;
    NodeID first_;
    Version version_;
    
    Logger * logger_;

    Clock * clock_;
    Timer * timer_;
    Rand * rand_;

    Callback * cb_;
    NetWork * net_;

    SpinLock * received_lock_;
    SpinLock * proposed_lock_;
    Queue<std::string> received_;
    Queue<ProposedValue> proposed_;

    std::set<NodeID> learners_;
    std::set<NodeID> proposers_;
    std::set<NodeID> acceptors_;

    Learner learner_;
    Proposer * proposer_;
    Acceptor * acceptor_;

    Config config_;
};

void NetWork::OnReceive(const std::string & msg, const Thread * thread)
{
    node_->OnReceive(msg, thread);
}

#pragma pack(push, 1)
struct PrepareMsg
{
    PrepareMsg(Version version, NodeID proposer, ProposalID id, unsigned int len)
        : type_(MSG_PREPARE), version_(version), proposer_(proposer), id_(id), len_(len) { }

    unsigned int type_;
    Version version_;
    NodeID proposer_;
    ProposalID id_;

    unsigned int len_;
    char instance_ids_[0];
};

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

struct RejectMsg
{
    RejectMsg(ProposalID max_id)
        : type_(MSG_REJECT), max_id_(max_id) { }

    unsigned int type_;
    ProposalID max_id_;
};

struct AcceptMsg
{
    AcceptMsg(Version version,
                unsigned int proposer, AcceptingID accept, ProposalID id, unsigned int len)
        : type_(MSG_ACCEPT), version_(version),
            proposer_(proposer), accept_(accept), id_(id), len_(len) { }

    unsigned int type_;
    Version version_;
    unsigned int proposer_;
    AcceptingID accept_;
    ProposalID id_;

    unsigned int len_;
    char values_[0];
};

struct AcceptReplyMsg
{
    AcceptReplyMsg(unsigned int acceptor, AcceptingID accept)
        : type_(MSG_ACCEPT_REPLY), acceptor_(acceptor), accept_(accept) { }

    unsigned int type_;
    unsigned int acceptor_;
    AcceptingID accept_;
};

struct LearnMsg
{
    LearnMsg(unsigned int proposer, LearningID learn, unsigned int len)
        : type_(MSG_LEARN), proposer_(proposer), learn_(learn), len_(len) { }

    unsigned int type_;
    unsigned int proposer_;
    LearningID learn_;

    unsigned int len_;
    char values_[0];
};

struct LearnReplyMsg
{
    LearnReplyMsg(unsigned int learner, LearningID learn)
        : type_(MSG_LEARN_REPLY), learner_(learner), learn_(learn) { }

    unsigned int type_;
    unsigned int learner_;
    LearningID learn_;
};
#pragma pack(pop)

class PrepareDelay : public Timeout
{
public:
    PrepareDelay(Proposer * proposer) : proposer_(proposer) { }
    virtual ~PrepareDelay() { }

    virtual void Process()
    {
        if (!canceled_)
            proposer_->DelaiedPrepare();
        delete this;
    }

private:
    Proposer * proposer_;
};

class PrepareRetryTimeout : public Timeout
{
public:
    PrepareRetryTimeout(Proposer * proposer, unsigned int count)
        : proposer_(proposer), count_(count) { }
    virtual ~PrepareRetryTimeout() { }

    virtual void Process()
    {
        if (!canceled_)
        {
            if (--count_ == 0)
            {
                proposer_->RestartPrepare();
                delete this;
            }
            else
                proposer_->Prepare();
        }
        else
            delete this;
    }

private:
    Proposer * proposer_;

    unsigned int count_;
};

class AcceptRetryTimeout : public Timeout
{
public:
    AcceptRetryTimeout(Proposer * proposer, AcceptingValues * values, unsigned int count)
        : proposer_(proposer), values_(values), count_(count) { }
    virtual ~AcceptRetryTimeout() { }

    virtual void Process()
    {
        if (!canceled_)
        {
            if (--count_ == 0)
            {
                proposer_->AcceptRejected();
                delete this;
            }
            else
                proposer_->Accept(values_);
        }
        else
            delete this;
    }

private:
    Proposer * proposer_;
    AcceptingValues * values_;
    unsigned int count_;
};

class LearnRetryTimeout : public Timeout
{
public:
    LearnRetryTimeout(Proposer * proposer, LearningValues * values)
        : proposer_(proposer), values_(values) { }
    virtual ~LearnRetryTimeout() { }

    virtual void Process()
    {
        if (!canceled_)
            proposer_->Learn(values_);
        else
            delete this;
    }

private:
    Proposer * proposer_;
    LearningValues * values_;
};

void Learner::OnLearn(const LearnMsg * msg)
{
    std::map<InstanceID, ProposalValue> values;
    ExtractProposalValues(node_->logger_, node_->thread_, msg->values_, msg->len_, &values);

    if (node_->proposer_)
    {
        node_->proposer_->OnLearn(values);
        if (node_->acceptor_) node_->acceptor_->OnLearn(values);
    }

    learned_values_.insert(values.begin(), values.end());

    std::map<InstanceID, ProposalValue> apply;
    while (learned_values_.find(next_id_to_apply_) != learned_values_.end())
        apply.insert(*learned_values_.find(next_id_to_apply_++));

    if (!apply.empty())
    {
        DEBUG(node_->logger_, node_->thread_, "apply: %s\n", Debug(apply).c_str());
        for (std::map<InstanceID, ProposalValue>::iterator it = apply.begin();
                it != apply.end();
                ++it)
            Apply(it->second.value_, NULL); // TODO return result
    }

    LearnReplyMsg reply(node_->id_, msg->learn_);
    std::string r((const char *)&reply, sizeof reply);

    DEBUG(node_->logger_, node_->thread_, "reply learn to %u for %llu\n", msg->proposer_, msg->learn_);
    node_->net_->Send(node_->thread_, msg->proposer_, r);
}

bool Learner::Apply(const Value & value, std::string * result)
{
    if (value.noop_)
        return false;
    else if (value.membership_changes_)
    {
        node_->ChangeMemberships(*value.membership_changes_);
        return false;
    }
    else
        return sm_->Apply(node_->thread_, value.value_, result);
}
Proposer::Proposer(NodeImpl * node)
    : node_(node),
        value_id_(0),
        max_proposal_id_(0), proposal_count_(0), proposal_id_(0),
        prepare_retry_timeout_(NULL), prepare_delay_(NULL),
        accepting_id_(0), learning_id_(0)
{
    StartPrepare();
}

Proposer::~Proposer()
{
    // We can't assert anything here
    // The learner removing takes effect whenever it is applied
    // by the proposer and the learner removed will not get the
    // message after that
    // It's not trivial to make things clean when the failover is
    // considered
    // We need to add more check somewhere else
    if (prepare_retry_timeout_)
    {
        if (prepare_delay_)
        {
            prepare_delay_->Cancel();
            delete prepare_retry_timeout_;
        }
        else
            prepare_retry_timeout_->Cancel();
        ASSERT(node_->logger_, node_->thread_, accepting_values_.empty());
    }
    else
        for (std::map<AcceptingID, AcceptingValues *>::iterator it = accepting_values_.begin();
                it != accepting_values_.end();
                ++it)
        {
            it->second->retry_timeout_->Cancel();
            delete it->second;
        }

    for (std::map<LearningID, LearningValues *>::iterator it = learning_values_.begin();
            it != learning_values_.end();
            ++it)
    {
        it->second->retry_timeout_->Cancel();
        delete it->second;
    }
}

void Proposer::Propose(const ProposedValue & proposed)
{
    DEBUG(node_->logger_, node_->thread_, "propose: %s\n", Debug(proposed).c_str());

    unlearned_proposed_values_.insert(std::make_pair(++value_id_, proposed));

    if (!prepare_retry_timeout_)
    {
        // Holes must be filled
        ASSERT(node_->logger_, node_->thread_, unproposed_instance_ids_.ids_.size() == 1);
        // To prevent committed value proposed again
        // Do not propose the value until the initially assigned instance id is commited
        // with different value
        // This is one place where assign the initial instance id
        InstanceID instance_id = unproposed_instance_ids_.Next();

        // Record value proposed by us, so if the final committed value differs,
        // we can propose again
        ASSERT(node_->logger_, node_->thread_,
                initial_proposals_.insert(std::make_pair(instance_id, value_id_)).second);

        std::map<InstanceID, ProposalValue> accept_values;
        ProposalValue value(proposal_id_, Value(node_->id_, value_id_, proposed));
        accept_values.insert(std::make_pair(instance_id, value));

        AcceptingValues * accept = new AcceptingValues(++accepting_id_, accept_values);
        accepting_values_.insert(std::make_pair(accepting_id_, accept));

        accept->retry_timeout_ = new AcceptRetryTimeout(this, accept, node_->config_.accept_retry_count_);
        Accept(accept);
    }
    else
        // This is the other place
        newly_proposed_values_.insert(value_id_);
}

void Proposer::OnPrepareReply(const PrepareReplyMsg * msg)
{
    if (!prepare_retry_timeout_ || msg->id_ != proposal_id_) return;

    // Proposal ID identifies prepare request and the prepare is
    // only responded by acceptors with the same version
    ASSERT(node_->logger_, node_->thread_, node_->acceptors_.find(msg->acceptor_) != node_->acceptors_.end());
    prepare_promised_.insert(msg->acceptor_);

    std::map<InstanceID, ProposalValue> values;
    ExtractProposalValues(node_->logger_, node_->thread_, msg->values_, msg->len_, &values);
    UpdateByPreAcceptedValues(values);

    if (prepare_promised_.size() >= node_->acceptors_.size() / 2 + 1)
    {
        // TODO add log

        prepare_promised_.clear();

        ASSERT(node_->logger_, node_->thread_, !prepare_delay_);
        prepare_retry_timeout_->Cancel();
        prepare_retry_timeout_ = NULL;

        ASSERT(node_->logger_, node_->thread_, accepting_values_.empty());

        unproposed_instance_ids_ = unlearned_instance_ids_;

        // Values to be accepted include
        // non-conflict values initially proposed by us,
        // newly proposed values and pre accepted values (ours
        // required, others optional?)
        // other uncommitted values should not be proposed
        // again (with different instance id) until it's initially
        // assigned instance id
        // is committed with different value as these values
        // might be accepted and proposed by others
        std::map<InstanceID, ProposalValue> accept_values;

        // This can be more strict by using first uncommitted value
        // when the prepare request is generated
        for (std::map<InstanceID, ProposalValue>::iterator it = pre_accepted_values_.begin();
                it != pre_accepted_values_.end();
                ++it)
        {
            // Don't compare to committed value if instance id is committed
            // as pre accepted values may not be up to date and will be rejected
            // anyway

            if (it->second.value_.proposer_ == node_->id_)
                // Values pre accepted by others can't be new
                ASSERT(node_->logger_, node_->thread_,
                        newly_proposed_values_.find(it->second.value_.value_id_) == newly_proposed_values_.end());

            // [OUTDATED] Don't have to remove committed values since
            // for those out-of-date values, other acceptors will reject
            // for those up-to-date values, other acceptors can simply
            // ignore as paxos ensure they are same

            // Due to id is removed in OnCommit, unproposed_instance_ids_
            // may not contain the pre-accepted value

            if (unproposed_instance_ids_.Contain(it->first))
            {
                unproposed_instance_ids_.Remove(it->first);
                ProposalValue value(proposal_id_, it->second.value_);
                ASSERT(node_->logger_, node_->thread_,
                        accept_values.insert(std::make_pair(it->first, value)).second);
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
                ProposalValue noop(proposal_id_, Value(node_->id_, ++value_id_));
                ASSERT(node_->logger_, node_->thread_,
                        accept_values.insert(std::make_pair(id, noop)).second);
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
            if (unproposed_instance_ids_.Contain(it->first))
            {
                // With this, initial proposals for a single instance id
                // won't be a set
                unproposed_instance_ids_.Remove(it->first);
                ASSERT(node_->logger_, node_->thread_,
                        unlearned_proposed_values_.find(it->second) != unlearned_proposed_values_.end());
                const ProposedValue & proposed = unlearned_proposed_values_.find(it->second)->second;
                ProposalValue value(proposal_id_, Value(node_->id_, it->second, proposed));
                ASSERT(node_->logger_, node_->thread_,
                        accept_values.insert(std::make_pair(it->first, value)).second);
            }

        for (std::set<ValueID>::iterator it = newly_proposed_values_.begin();
                it != newly_proposed_values_.end();
                ++it)
        {
            InstanceID instance_id = unproposed_instance_ids_.Next();
            ASSERT(node_->logger_, node_->thread_,
                    initial_proposals_.insert(std::make_pair(instance_id, *it)).second);

            ASSERT(node_->logger_, node_->thread_,
                    unlearned_proposed_values_.find(*it) != unlearned_proposed_values_.end());
            const ProposedValue & proposed = unlearned_proposed_values_.find(*it)->second;
            ProposalValue value(proposal_id_, Value(node_->id_, *it, proposed));
            ASSERT(node_->logger_, node_->thread_,
                    accept_values.insert(std::make_pair(instance_id, value)).second);
        }
        newly_proposed_values_.clear();

        if (!accept_values.empty())
        {
            AcceptingValues * accept = new AcceptingValues(++accepting_id_, accept_values);
            accepting_values_.insert(std::make_pair(accepting_id_, accept));
            accept->retry_timeout_ = new AcceptRetryTimeout(this, accept,
                                                            node_->config_.accept_retry_count_);
            Accept(accept);
        }

        if (!node_->learner_.LearnedValues().empty())
        {
            std::map<InstanceID, ProposalValue> values = node_->learner_.LearnedValues();
            LearningValues * learn = new LearningValues(++learning_id_, values);
            learning_values_.insert(std::make_pair(learning_id_, learn));
            learning_values_for_acceptors_.insert(std::make_pair(learning_id_, std::set<NodeID>()));
            learn->retry_timeout_ = new LearnRetryTimeout(this, learn);
            Learn(learn);
        }
    }
}

void Proposer::OnReject(const RejectMsg * msg)
{
    if (max_proposal_id_ < msg->max_id_)
        max_proposal_id_ = msg->max_id_;
}

void Proposer::OnAcceptReply(const AcceptReplyMsg * msg)
{
    if (accepting_values_.find(msg->accept_) == accepting_values_.end()) return;

    AcceptingValues * accept = accepting_values_.find(msg->accept_)->second;

    ASSERT(node_->logger_, node_->thread_,
            node_->acceptors_.find(msg->acceptor_) != node_->acceptors_.end());

    accept->accepted_.insert(msg->acceptor_);
    if (accept->accepted_.size() >= node_->acceptors_.size() / 2 + 1)
    {
        for (std::map<InstanceID, ProposalValue>::const_iterator it = accept->values_.begin();
                it != accept->values_.end();
                ++it)
            node_->cb_->Accepted(node_->thread_, it->second.value_.cb_);

        LearningValues * learn = new LearningValues(++learning_id_, accept->values_);
        learning_values_.insert(std::make_pair(learning_id_, learn));
        learn->retry_timeout_ = new LearnRetryTimeout(this, learn);
        Learn(learn);

        accept->retry_timeout_->Cancel();
        accepting_values_.erase(msg->accept_);
        delete accept;
    }
}

void Proposer::OnLearnReply(const LearnReplyMsg * msg)
{
    if (learning_values_.find(msg->learn_) == learning_values_.end()) return;

    DEBUG(node_->logger_, node_->thread_, "learn replied from %u for %llu\n", msg->learner_, msg->learn_);

    LearningValues * learn = learning_values_.find(msg->learn_)->second;

    learn->learned_.insert(msg->learner_);

    if (learning_values_for_acceptors_.find(msg->learn_) != learning_values_for_acceptors_.end()
        && node_->acceptors_.find(msg->learner_) != node_->acceptors_.end())
    {
        learning_values_for_acceptors_.find(msg->learn_)->second.insert(msg->learner_);
        DEBUG(node_->logger_, node_->thread_,
                "learned acceptors: %u/%u\n",
                learning_values_for_acceptors_.find(msg->learn_)->second.size(),
                node_->acceptors_.size() / 2 + 1);
        if (learning_values_for_acceptors_.find(msg->learn_)->second.size() >= node_->acceptors_.size() / 2 + 1)
        {
            for (std::map<InstanceID, ProposalValue>::iterator it = learn->values_.begin();
                    it != learn->values_.end();
                    ++it)
                node_->cb_->Applied(node_->thread_, it->second.value_.cb_, NULL); // TODO pass result
            learning_values_for_acceptors_.erase(msg->learn_);
        }
    }

    if (learn->learned_.size() == node_->learners_.size())
    {
        ASSERT(node_->logger_, node_->thread_,
                learning_values_for_acceptors_.find(msg->learn_) == learning_values_for_acceptors_.end());
        learn->retry_timeout_->Cancel();
        learning_values_.erase(msg->learn_);
        delete learn;
    }
}

void Proposer::OnLearn(const std::map<InstanceID, ProposalValue> & values)
{
    const std::map<InstanceID, ProposalValue> & learned_values = node_->learner_.LearnedValues();
    std::set<InstanceID> conflicts;
    for (std::map<InstanceID, ProposalValue>::const_iterator it = values.begin();
            it != values.end();
            ++it)
    {
        if (learned_values.find(it->first) != learned_values.end())
        {
            Value v = learned_values.find(it->first)->second.value_;
            ASSERT(node_->logger_, node_->thread_,
                    it->second.value_ == learned_values.find(it->first)->second.value_);
        }

        if (learned_values.find(it->first) == learned_values.end()
            && it->second.value_.proposer_ == node_->id_
            && !it->second.value_.noop_)
            ASSERT(node_->logger_, node_->thread_,
                    unlearned_proposed_values_.find(it->second.value_.value_id_) != unlearned_proposed_values_.end());

        if (learned_values.find(it->first) == learned_values.end())
        {
            ASSERT(node_->logger_, node_->thread_, unlearned_instance_ids_.Contain(it->first));
            unlearned_instance_ids_.Remove(it->first);
        }

        // We can't propose to the committed instance id as
        // it's committed and it may not be committed again,
        // while we need to check the unproposed instance ids as
        // the id may already be used by a new value
        // and in this case, the new value will be re-proposed
        // with another id if there is a confliction
        if (unproposed_instance_ids_.Contain(it->first))
            unproposed_instance_ids_.Remove(it->first);

        if (it->second.value_.proposer_ == node_->id_
            && unlearned_proposed_values_.find(it->second.value_.value_id_) != unlearned_proposed_values_.end())
        {
            ASSERT(node_->logger_, node_->thread_,
                    initial_proposals_.find(it->first) != initial_proposals_.end());
            unlearned_proposed_values_.erase(it->second.value_.value_id_);
        }

        if (initial_proposals_.find(it->first) != initial_proposals_.end())
        {
            ValueID value_id = initial_proposals_.find(it->first)->second;
            if (it->second.value_.proposer_ != node_->id_
                || it->second.value_.value_id_ != value_id)
            {
                ASSERT(node_->logger_, node_->thread_,
                        unlearned_proposed_values_.find(value_id) != unlearned_proposed_values_.end());
                conflicts.insert(value_id);
            }
            initial_proposals_.erase(it->first);
        }
    }

    if (!conflicts.empty())
    {
        if (!prepare_retry_timeout_)
        {
            std::map<InstanceID, ProposalValue> accept_values;
            for (std::set<ValueID>::iterator it = conflicts.begin();
                    it != conflicts.end();
                    ++it)
            {
                InstanceID instance_id = unproposed_instance_ids_.Next();
                ASSERT(node_->logger_, node_->thread_,
                        initial_proposals_.insert(std::make_pair(instance_id, *it)).second);
                const ProposedValue & proposed = unlearned_proposed_values_.find(*it)->second;
                ProposalValue value(proposal_id_, Value(node_->id_, *it, proposed));
                accept_values.insert(std::make_pair(instance_id, value));
            }

            AcceptingValues * accept = new AcceptingValues(++accepting_id_, accept_values);
            accepting_values_.insert(std::make_pair(accepting_id_, accept));
            accept->retry_timeout_ = new AcceptRetryTimeout(this, accept,
                                                            node_->config_.accept_retry_count_);
            Accept(accept);
        }
        else
            for (std::set<ValueID>::iterator it = conflicts.begin();
                    it != conflicts.end();
                    ++it)
                ASSERT(node_->logger_, node_->thread_, newly_proposed_values_.insert(*it).second);
    }
}

void Proposer::LearnersChanged()
{
    if (!prepare_retry_timeout_)
    {
        std::map<InstanceID, ProposalValue> values = node_->learner_.LearnedValues();

        for (std::map<LearningID, LearningValues *>::iterator it = learning_values_.begin();
                it != learning_values_.end();
                ++it)
        {
            values.insert(it->second->values_.begin(), it->second->values_.end());
            it->second->retry_timeout_->Cancel();
        }
        learning_values_.clear();
        learning_values_for_acceptors_.clear();
        LearningValues * learn = new LearningValues(++learning_id_, values);
        learning_values_.insert(std::make_pair(learning_id_, learn));
        learning_values_for_acceptors_.insert(std::make_pair(learning_id_, std::set<NodeID>()));
        learn->retry_timeout_ = new LearnRetryTimeout(this, learn);
        Learn(learn);
    }
    else
    {
        for (std::map<LearningID, LearningValues *>::iterator it = learning_values_.begin();
                it != learning_values_.end();
                ++it)
            it->second->retry_timeout_->Cancel();
        learning_values_.clear();
        learning_values_for_acceptors_.clear();
    }
}

void Proposer::AcceptorsChanged(bool add, NodeID node)
{
    std::set<LearningID> acceptors_learned;
    for (std::map<LearningID, std::set<NodeID> >::iterator it = learning_values_for_acceptors_.begin();
            it != learning_values_for_acceptors_.end();
            ++it)
    {
        if (!add && it->second.find(node) != it->second.end())
            it->second.erase(node);

        ASSERT(node_->logger_, node_->thread_,
                learning_values_.find(it->first) != learning_values_.end());
        LearningValues * learn = learning_values_.find(it->first)->second;
        // TODO consider more carefully
        if (add && learn->learned_.find(node) != learn->learned_.end())
            it->second.insert(node);

        if (it->second.size() >= node_->acceptors_.size() / 2 + 1)
        {
            for (std::map<InstanceID, ProposalValue>::iterator jt = learn->values_.begin();
                    jt != learn->values_.end();
                    ++jt)
                node_->cb_->Applied(node_->thread_, jt->second.value_.cb_, NULL); // TODO pass result
            acceptors_learned.insert(it->first);
        }
    }
    for (std::set<LearningID>::iterator it = acceptors_learned.begin();
            it != acceptors_learned.end();
            ++it)
        learning_values_for_acceptors_.erase(*it);

    if (prepare_retry_timeout_) // TODO
    {
        if (prepare_delay_)
        {
            prepare_delay_->Cancel();
            prepare_delay_ = NULL;
            delete prepare_retry_timeout_;
        }
        else
            prepare_retry_timeout_->Cancel();
        RestartPrepare();
    }
    else
        AcceptRejected();
}

void Proposer::StartPrepare()
{
    ASSERT(node_->logger_, node_->thread_, prepare_retry_timeout_ == NULL);
    ASSERT(node_->logger_, node_->thread_, prepare_promised_.empty());
    ASSERT(node_->logger_, node_->thread_, pre_accepted_values_.empty());

    UpdateProposalID();

    preparing_instance_ids_ = unlearned_instance_ids_;
    prepare_retry_timeout_ = new PrepareRetryTimeout(this, node_->config_.prepare_retry_count_);

    TimeStamp now = node_->clock_->Now(node_->thread_);
    TimeStamp future = now + node_->rand_->Randomize(node_->config_.prepare_delay_min_, node_->config_.prepare_delay_max_);
    DEBUG(node_->logger_, node_->thread_, "add restart prepare timer: now = %llu, future = %llu\n", now, future);
    prepare_delay_ = new PrepareDelay(this);
    node_->timer_->AddTimeout(prepare_delay_, future);
}

void Proposer::UpdateProposalID()
{
    proposal_id_ = ((++proposal_count_) << 16) | node_->id_;
    while (proposal_id_ < max_proposal_id_)
        proposal_id_ = ((++proposal_count_) << 16) | node_->id_;
}

void Proposer::RestartPrepare()
{
    prepare_retry_timeout_ = NULL;
    prepare_promised_.clear();
    pre_accepted_values_.clear();
    StartPrepare();
}

void Proposer::DelaiedPrepare()
{
    prepare_delay_ = NULL;
    Prepare();
}

void Proposer::Prepare()
{
    DEBUG(node_->logger_, node_->thread_,
            "broadcast prepare with version %u: <%llu> %s\n",
            node_->version_, proposal_id_,
            preparing_instance_ids_.ToString().c_str());

    unsigned int len = CalcAvailableInstanceIDsLength(&preparing_instance_ids_);
    unsigned int size = sizeof (PrepareMsg) + len;

    PrepareMsg * msg = new (new char[size]) PrepareMsg(node_->version_, node_->id_, proposal_id_, len);
    FillAvailableInstanceIDs(msg->instance_ids_, &preparing_instance_ids_);
    std::string m((const char *)msg, size);
    delete [] (char *)msg;

    for (std::set<NodeID>::iterator it = node_->acceptors_.begin();
            it != node_->acceptors_.end();
            ++it)
        node_->net_->Send(node_->thread_, *it, m);

    node_->timer_->AddTimeout(prepare_retry_timeout_,
                                node_->clock_->Now(node_->thread_) + node_->config_.prepare_retry_timeout_);
}

void Proposer::UpdateByPreAcceptedValues(const std::map<InstanceID, ProposalValue> & values)
{
    DEBUG(node_->logger_, node_->thread_,
            "update by pre-accepted values: %s\n", Debug(values).c_str());

    for (std::map<InstanceID, ProposalValue>::const_iterator it = values.begin();
            it != values.end();
            ++it)
        if (pre_accepted_values_.find(it->first) != pre_accepted_values_.end())
        {
            if (it->second.proposal_id_ > pre_accepted_values_.find(it->first)->second.proposal_id_)
                pre_accepted_values_.find(it->first)->second = it->second;
        }
        else
            pre_accepted_values_.insert(*it);
}

void Proposer::Accept(AcceptingValues * accept)
{
    DEBUG(node_->logger_, node_->thread_,
            "broadcast accept: %s\n", Debug(accept->values_).c_str());

    unsigned int len = CalcProposalValuesLength(accept->values_);
    unsigned int size = sizeof (AcceptMsg) + len;

    for (std::map<InstanceID, ProposalValue>::iterator it = accept->values_.begin();
            it != accept->values_.end();
            ++it)
        ASSERT(node_->logger_, node_->thread_, it->second.proposal_id_ == proposal_id_);

    AcceptMsg * msg = new (new char[size]) AcceptMsg(node_->version_, node_->id_,
                                                        accept->id_, proposal_id_, len);
    FillProposalValues(msg->values_, accept->values_);
    std::string m((const char *)msg, size);
    delete [] (char *)msg;

    for (std::set<NodeID>::iterator it = node_->acceptors_.begin();
            it != node_->acceptors_.end();
            ++it)
        if (accept->accepted_.find(*it) == accept->accepted_.end())
            node_->net_->Send(node_->thread_, *it, m);

    node_->timer_->AddTimeout(accept->retry_timeout_,
                                node_->clock_->Now(node_->thread_) + node_->config_.accept_retry_timeout_);
}

void Proposer::AcceptRejected()
{
    DEBUG(node_->logger_, node_->thread_, "accept rejected\n");

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

void Proposer::Learn(LearningValues * learn)
{
    DEBUG(node_->logger_, node_->thread_,
            "broadcast learn: %s\n", Debug(learn->values_).c_str());

    unsigned int len = CalcProposalValuesLength(learn->values_);
    unsigned int size = sizeof (LearnMsg) + len;

    LearnMsg * msg = new (new char[size]) LearnMsg(node_->id_, learn->id_, len);
    FillProposalValues(msg->values_, learn->values_);
    std::string m((const char *)msg, size);
    delete [] (char *)msg;

    for (std::set<NodeID>::iterator it = node_->learners_.begin();
            it != node_->learners_.end();
            ++it)
        if (learn->learned_.find(*it) == learn->learned_.end())
            node_->net_->Send(node_->thread_, *it, m);

    node_->timer_->AddTimeout(learn->retry_timeout_,
                                node_->clock_->Now(node_->thread_) + node_->config_.learn_retry_timeout_);
}

void Acceptor::OnPrepare(const PrepareMsg * msg)
{
    if (msg->version_ != node_->version_) return;

    DEBUG(node_->logger_, node_->thread_,
            "proposal id: %llu, promised proposal id: %llu\n",
            msg->id_, promised_proposal_id_);

    if (msg->id_ > max_proposal_id_)
        max_proposal_id_ = msg->id_;

    if (msg->id_ > promised_proposal_id_)
    {
        promised_proposal_id_ = msg->id_;

        AvailableInstanceIDs instance_ids;
        ExtractAvailableInstanceIDs(node_->logger_, node_->thread_,
                                    msg->instance_ids_, msg->len_, &instance_ids);

        std::map<InstanceID, ProposalValue> values;
        FilterAcceptedValues(&instance_ids, &values);

        DEBUG(node_->logger_, node_->thread_,
                "reply prepare to %u: %s\n", msg->proposer_, Debug(values).c_str());

        unsigned int len = CalcProposalValuesLength(values);
        unsigned int size = sizeof (PrepareReplyMsg) + len;
        PrepareReplyMsg * reply = new (new char[size]) PrepareReplyMsg(node_->id_, msg->id_, len);
        FillProposalValues(reply->values_, values);
        std::string r((const char *)reply, size);
        delete [] (char *)reply;

        node_->net_->Send(node_->thread_, msg->proposer_, r);
    }
    else if (msg->id_ < promised_proposal_id_)
    {
        RejectMsg reject(max_proposal_id_);
        std::string r((const char *)&reject, sizeof reject);
        node_->net_->Send(node_->thread_, msg->proposer_, r);
    }
}

void Acceptor::OnAccept(const AcceptMsg * msg)
{
    if (msg->version_ != node_->version_) return;

    DEBUG(node_->logger_, node_->thread_,
            "promised proposal id: %llu\n", promised_proposal_id_);

    if (msg->id_ > max_proposal_id_)
        max_proposal_id_ = msg->id_;

    if (msg->id_ >= promised_proposal_id_)
    {
        std::map<InstanceID, ProposalValue> values;
        ExtractProposalValues(node_->logger_, node_->thread_, msg->values_, msg->len_, &values);

        DEBUG(node_->logger_, node_->thread_,
                "accept values from %u: %s\n", msg->proposer_, Debug(values, &accepted_values_).c_str());

        const std::map<InstanceID, ProposalValue> & learned_values = node_->learner_.LearnedValues();
        for (std::map<InstanceID, ProposalValue>::iterator it = values.begin();
                it != values.end();
                ++it)
            if (learned_values.find(it->first) == learned_values.end())
                accepted_values_.insert(*it);
            else
                ASSERT(node_->logger_, node_->thread_,
                        it->second.value_ == learned_values.find(it->first)->second.value_);

        AcceptReplyMsg reply(node_->id_, msg->accept_);
        std::string r((const char *)&reply, sizeof reply);

        DEBUG(node_->logger_, node_->thread_,
                "reply accept to %u for %llu\n", msg->proposer_, msg->accept_);

        node_->net_->Send(node_->thread_, msg->proposer_, r);
    }
    else
    {
        RejectMsg reject(max_proposal_id_);
        std::string r((const char *)&reject, sizeof reject);
        node_->net_->Send(node_->thread_, msg->proposer_, r);
    }
}

void Acceptor::OnLearn(const std::map<InstanceID, ProposalValue> & values)
{
    for (std::map<InstanceID, ProposalValue>::const_iterator it = values.begin();
            it != values.end();
            ++it)
        if (accepted_values_.find(it->first) != accepted_values_.end())
            accepted_values_.erase(it->first);
}

static void FilterAcceptedInstances(Logger * logger, Thread * thread,
                                    const std::map<InstanceID, ProposalValue> & values,
                                    InstanceID a, InstanceID b,
                                    std::map<InstanceID, ProposalValue> * result)
{
    for (std::map<InstanceID, ProposalValue>::const_iterator it = values.lower_bound(a);
            it != values.lower_bound(b);
            ++it)
        ASSERT(logger, thread, result->insert(*it).second);
}

void Acceptor::FilterAcceptedValues(const AvailableInstanceIDs * ids,
                                    std::map<InstanceID, ProposalValue> * values) const
{
    for (std::set<std::pair<InstanceID, InstanceID> >::const_iterator it = ids->ids_.begin();
            it != ids->ids_.end();
            ++it)
    {
        FilterAcceptedInstances(node_->logger_, node_->thread_,
                                accepted_values_, it->first, it->second, values);
        FilterAcceptedInstances(node_->logger_, node_->thread_,
                                node_->learner_.LearnedValues(), it->first, it->second, values);
    }
}

static void * RunNode(void * arg)
{
    ((NodeImpl *)arg)->Loop();
    return NULL;
}

NodeImpl::NodeImpl(Thread * thread, NodeID id, NodeID first,
                    Logger * logger,
                    Clock * clock, Timer * timer, Rand * rand,
                    Callback * cb, NetWork * net, StateMachine * sm,
                    const Config & config)
    : thread_(NULL),
        stop_(thread->NewAtomicBool(false)),
        proposered_(false), id_(id), first_(first), version_(0),
        logger_(logger), clock_(clock), timer_(timer), rand_(rand),
        cb_(cb), net_(net),
        received_lock_(thread->NewSpinLock()), proposed_lock_(thread->NewSpinLock()),
        received_(Queue<std::string>(received_lock_)),
        proposed_(Queue<ProposedValue>(proposed_lock_)),
        learner_(this, sm), proposer_(NULL), acceptor_(NULL),
        config_(config) { }

NodeImpl::~NodeImpl()
{
    delete proposed_lock_;
    delete received_lock_;
    delete stop_;
}

void NodeImpl::Init(const std::string & thread_prefix, Thread * thread)
{
    net_->Init(this);
    thread->NewThread(&thread_, RunNode, this, thread_prefix + "-paxos");
}

void NodeImpl::Fini(const Thread * thread)
{
    stop_->Set(true, thread);
    thread_->Join();
    delete thread_;
    thread_ = NULL;
    net_->Fini();
}

void NodeImpl::ChangeMemberships(const std::vector<MembershipChange> & changes)
{
    // TODO add log

    for (std::vector<MembershipChange>::const_iterator it = changes.begin();
            it != changes.end();
            ++it)
    {
        if (it->type_ == ADD_LEARNER)
        {
            ASSERT(logger_, thread_, learners_.insert(it->node_).second);
            if (proposer_)
                proposer_->LearnersChanged();

            if (it->node_ == id_)
                ASSERT(logger_, thread_, !proposer_ && !acceptor_);
        }
        else if (it->type_ == LEARNER_TO_PROPOSER)
        {
            ASSERT(logger_, thread_, proposers_.insert(it->node_).second);
            if (it->node_ == id_)
            {
                ASSERT(logger_, thread_, !proposered_);
                proposered_ = true;

                ASSERT(logger_, thread_, !proposer_ && !acceptor_);
                proposer_ = new Proposer(this);
            }
        }
        else if (it->type_ == PROPOSER_TO_ACCEPTOR)
        {
            ASSERT(logger_, thread_, acceptors_.insert(it->node_).second);
            ++version_;
            if (proposer_)
                proposer_->AcceptorsChanged(true, it->node_);

            if (it->node_ == id_)
            {
                ASSERT(logger_, thread_, proposer_ && !acceptor_);
                acceptor_ = new Acceptor(this);
            }
        }
        else if (it->type_ == DEL_LEARNER)
        {
            ASSERT(logger_, thread_, learners_.find(it->node_) != learners_.end());
            learners_.erase(it->node_);
            if (proposer_)
                proposer_->LearnersChanged();

            if (it->node_ == id_)
                ASSERT(logger_, thread_, !proposer_ && !acceptor_);
        }
        else if (it->type_ == PROPOSER_TO_LEARNER)
        {
            ASSERT(logger_, thread_, proposers_.find(it->node_) != proposers_.end());
            proposers_.erase(it->node_);
            if (it->node_ == id_)
            {
                ASSERT(logger_, thread_, proposer_ && !acceptor_);
                // Without failover and error of conflication,
                // currently this depends 
                // on the clients confirmed every proposals
                // proposed before removing
                // Otherwise, if we still have uncommitted values
                // and want to commit these values before
                // close the proposer, we need to still be a
                // learner to get the newest acceptor list
                // and therefore other nodes need to know
                // we are still a learner and can't be removed
                // before we close ourself, then a request
                // generated by us is required to inform other
                // nodes when is safe to remove us, and
                // due to possible crashes, this request
                // may fail and the removing may stuck and
                // there is no simple clean way to handle
                // this
                delete proposer_;
                proposer_ = NULL;
            }
        }
        else if (it->type_ == ACCEPTOR_TO_PROPOSER)
        {
            ASSERT(logger_, thread_, acceptors_.find(it->node_) != acceptors_.end());
            ASSERT(logger_, thread_, acceptors_.size() != 1);

            acceptors_.erase(it->node_);
            ++version_;
            if (proposer_)
                proposer_->AcceptorsChanged(false, it->node_);

            if (it->node_ == id_)
            {
                ASSERT(logger_, thread_, proposer_ && acceptor_);
                delete acceptor_;
                acceptor_ = NULL;
            }
        }
        else
            ASSERT(logger_, thread_, false);
    }
}

Node::Node(Thread * thread, NodeID id, NodeID first,
            Logger * logger, Clock * clock, Timer * timer, Rand * rand,
            Callback * cb, NetWork * net, StateMachine * sm,
            const Config & config)
    : impl_(new NodeImpl(thread, id, first,
                            logger, clock, timer, rand, cb, net, sm, config)) { }
Node::~Node() { delete impl_; }

void Node::Init(const std::string & thread_prefix, Thread * thread)
{
    impl_->Init(thread_prefix, thread);
}

void Node::Fini(const Thread * thread)
{
    impl_->Fini(thread);
}

void Node::Propose(const std::string & value, const std::string & cb, const Thread * thread)
{
    impl_->Propose(value, cb, thread);
}

void Node::AddLearner(NodeID id, const std::string &cb, const Thread * thread)
{
    impl_->AddLearner(id, cb, thread);
}

void Node::AddProposer(NodeID id, const std::string & cb, const Thread * thread)
{
    impl_->AddProposer(id, cb, thread);
}

void Node::AddAcceptor(NodeID id, const std::string & cb, const Thread * thread)
{
    impl_->AddAcceptor(id, cb, thread);
}

void Node::LearnerToProposer(NodeID id, const std::string & cb, const Thread * thread)
{
    impl_->LearnerToProposer(id, cb, thread);
}

void Node::LearnerToAcceptor(NodeID id, const std::string & cb, const Thread * thread)
{
    impl_->LearnerToAcceptor(id, cb, thread);
}

void Node::ProposerToAcceptor(NodeID id, const std::string & cb, const Thread * thread)
{
    impl_->ProposerToAcceptor(id, cb, thread);
}

void Node::DelLearner(NodeID id, const std::string & cb, const Thread * thread)
{
    impl_->DelLearner(id, cb, thread);
}

void Node::DelProposer(NodeID id, const std::string & cb, const Thread * thread)
{
    impl_->DelProposer(id, cb, thread);
}

void Node::DelAcceptor(NodeID id, const std::string & cb, const Thread * thread)
{
    impl_->DelAcceptor(id, cb, thread);
}

void Node::ProposerToLearner(NodeID id, const std::string & cb, const Thread * thread)
{
    impl_->ProposerToLearner(id, cb, thread);
}

void Node::AcceptorToLearner(NodeID id, const std::string & cb, const Thread * thread)
{
    impl_->AcceptorToLearner(id, cb, thread);
}

void Node::AcceptorToProposer(NodeID id, const std::string & cb, const Thread * thread)
{
    impl_->AcceptorToProposer(id, cb, thread);
}

}
