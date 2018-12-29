#ifndef RADIXHASHJOIN_H
#define RADIXHASHJOIN_H

#include "Relation.h"
#include "JoinResults.h"
#include "JobScheduler.h"


void setH(unsigned int _H1_N,unsigned int _H2_N);
Result *radixHashJoin(JoinRelation &R, JoinRelation &S);
bool indexRelation(intField *bucketJoinField, unsigned int bucketSize, unsigned int *&chain, unsigned int *&table);
bool probeResults(const intField *LbucketJoinField, const unsigned int *LbucketRowIds, const intField *IbucketJoinField, const unsigned int *IbucketRowIds, const unsigned int *chain, const unsigned int *H2HashTable, unsigned int bucketSize, Result *result, bool saveLfirst);


class JoinJob : public Job {
    const unsigned int bucket_num;   // the bucket of I and L for which to perform phases 2 and 3 of radix hash join
    JoinRelation &I, &L;             // I = Indexed JoinRelation, L = not indexed JoinRelation
    Result *result;
    const bool saveLfirst;
public:
    JoinJob(const unsigned int bucket, JoinRelation &_I, JoinRelation &_L, Result *_result, const bool _saveLfirst);
    bool run();
};


class HistJob : public Job {
    const intField *joinField;
    const unsigned int start, end, H1_N;
    unsigned int *Hist;
    unsigned int *bucket_nums;
public:
    HistJob(const intField *_joinField, unsigned int _start, unsigned int _end, unsigned int *_Hist, unsigned int *_bucket_nums, unsigned int _H1_N);
    bool run();
};


class PartitionJob : public Job {

public:
    PartitionJob();
    bool run();
};

#endif
