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
    const unsigned int num_of_buckets;
    pthread_mutex_t *Hist_bucket_locks;   // one lock per bucket i to protect Hist[i]
public:
    HistJob(const intField *_joinField, unsigned int _start, unsigned int _end, unsigned int *_Hist, unsigned int *_bucket_nums, unsigned int _H1_N);
    ~HistJob();
    bool run();
};


class PartitionJob : public Job {
    intField *newJoinField, *oldJoinField;
    unsigned int *newRowids, *oldRowids;
    unsigned int *nextBucketPos;
    const unsigned int start, end, num_of_buckets;
    const unsigned int *bucket_nums, *Psum;
    pthread_mutex_t *bucket_pos_locks;   // one lock per bucket i to protect nextBocketPos[i]
public:
    PartitionJob(intField *_newJoinField, intField *_oldJoinField, unsigned int *_newRowids, unsigned int *_oldRowids, unsigned int *_nextBucketPos, unsigned int _start, unsigned int _end, unsigned int _num_of_buckets, const unsigned int *_bucket_nums, unsigned int *_Psum);
    ~PartitionJob();
    bool run();
};

#endif
