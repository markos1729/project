#include <iostream>
#include <cstring>
#include <cmath>
#include <pthread.h>
#include "../Headers/RadixHashJoin.h"
#include "../Headers/HashFunctions.h"
#include "../Headers/macros.h"


/// choose a partition parallel scheme:
//#define PARALLEL_IMPLEMENTATION_OF_PARTITION_INSTEAD_OF_THEIR_CALL


using namespace std;


extern JobScheduler *scheduler;


/* Globals */
unsigned int H1_N, H2_N;


/* Local Functions & structs */
void setH(unsigned int _H1_N, unsigned int _H2_N) { H1_N = _H1_N; H2_N = _H2_N; }

#ifndef PARALLEL_IMPLEMENTATION_OF_PARTITION_INSTEAD_OF_THEIR_CALL
struct partition_args{
    JoinRelation &R;
    unsigned int H1_N;
    partition_args(JoinRelation &_R, unsigned int _H1_N) : R(_R), H1_N(_H1_N) { }
};

void *thread_partition(void *args);
#endif


Result* radixHashJoin(JoinRelation &R, JoinRelation &S) {
    CHECK( scheduler != NULL, "JobScheduler is not initiated: cannot run parallel RHJ", return NULL; )
    // Partition R and S, whilst keeping a 'Psum' table for each bucket in R and S (phase 1)
    H1_N = (unsigned int) ( ceil( log2( MAX(R.getSize(), S.getSize()) / CACHE ))); // H1_N is the same for both Relations rounded up  TODO: I think ceil() does not work properly!
    H2_N = H1_N/2;
#ifdef PARALLEL_IMPLEMENTATION_OF_PARTITION_INSTEAD_OF_THEIR_CALL
    CHECK( R.partitionRelation(H1_N) , "partitioning R failed", return NULL; )
    CHECK( S.partitionRelation(H1_N) , "partitioning S failed", return NULL; )
#else
    // R will be partitioned by the main thread whilst S by a new thread
    bool fail = false;
    pthread_t tid;
    struct partition_args args(S, H1_N);
    CHECK_PERROR(pthread_create(&tid, NULL, thread_partition, (void *) &args), "pthread_create failed", fail = true; )
    if (fail) {    // should not happen
        CHECK( R.partitionRelationSequentially(H1_N) , "partitioning R failed", return NULL; )
        CHECK( S.partitionRelationSequentially(H1_N) , "partitioning S failed", return NULL; )
    } else {
        CHECK( R.partitionRelationSequentially(H1_N) , "partitioning R failed", return NULL; )
        CHECK_PERROR(pthread_join(tid, NULL), "pthread_create failed", fail = true; )
    }
#endif
    // Choose one of them for indexing, lets say I, and keep the other for scanning, lets say L
    bool saveLfirst;
    JoinRelation *L = NULL, *I = NULL;
    if (R.getSize() < S.getSize()){   // index the smaller of the two Relations
        I = &R;
        L = &S;
        saveLfirst = true;
    } else{
        I = &S;
        L = &R;
        saveLfirst = false;
    }
    // Define Result lists object to fill
    Result *result = new Result;
    Result *tempResults = new Result[I->getNumberOfBuckets() - 1];
    // Iteratively perform phases 2 and 3 for all buckets of both similarly partitioned Relations and add results bucket by bucket
    scheduler->schedule(new JoinJob(0, *I, *L, result, saveLfirst));
    for (unsigned int i = 1 ; i < I->getNumberOfBuckets() ; i++) {
		scheduler->schedule(new JoinJob(i, *I, *L, &tempResults[i - 1], saveLfirst));    // (!) this will be virtually deleted by the JobScheduler
	}
	// wait for RHJ to finish (IMPORTANT)
    scheduler->waitUntilAllJobsHaveFinished();
    // and merge results to one big result list to return
    for (unsigned int i = 1 ; i < I->getNumberOfBuckets() ; i++) {
        result->addList(&tempResults[i - 1]);
    }
    delete[] tempResults;
    return result;
}

/* Local Function Implementation */
// phase 2: index I's given bucket by creating 'H2HashTable' and 'chain' structures
bool indexRelation(intField *bucketJoinField, unsigned int bucketSize, unsigned int *&chain, unsigned int *&table){
	unsigned int sz = (unsigned int) 0x01 << H2_N;  // = 2^H2_N
    table = new unsigned int[sz];
    chain = new unsigned int[bucketSize];
    unsigned int *last = new unsigned int[sz];
    
    memset(table, 0, sz * sizeof(unsigned int));
    memset(chain, 0, bucketSize * sizeof(unsigned int));
    for (unsigned int i = bucketSize ; i > 0 ; --i) {
        unsigned int h = H2(bucketJoinField[i - 1], H1_N, H2_N);
        if (table[h] == 0) last[h] = table[h] = i;
        else last[h] = chain[last[h] - 1] = i;
    }
    
    delete[] last;
    return true;
}

// phase 3: probe linearly L's given bucket and add results based on equal values in indexed I's given bucket
bool probeResults(const intField *LbucketJoinField, const unsigned int *LbucketRowIds,
                  const intField *IbucketJoinField, const unsigned int *IbucketRowIds,
                  const unsigned int *chain, const unsigned int *H2HashTable,
                  unsigned int LbucketSize, Result *result, bool saveLfirst){
	for (unsigned int i = 0; i < LbucketSize; i++) {
		unsigned int h = H2(LbucketJoinField[i], H1_N, H2_N);
		if (H2HashTable[h] == 0) continue;		// no records in I with that value
		unsigned int chainIndex = H2HashTable[h];
		while (chainIndex > 0) {
			if (LbucketJoinField[i] == IbucketJoinField[chainIndex - 1]) {
			    if (saveLfirst) result->addTuple(IbucketRowIds[chainIndex - 1], LbucketRowIds[i]);
			    else result->addTuple(LbucketRowIds[i], IbucketRowIds[chainIndex - 1]);
			}
			chainIndex = chain[chainIndex - 1];
		}
	}
	return true;
}



/***********************************/
/*** Parallel Job Implementation ***/
/***********************************/
/* Join Job */
JoinJob::JoinJob(const unsigned int bucket, JoinRelation &_I, JoinRelation &_L, Result *_result, const bool _saveLfirst)
        : Job(), bucket_num(bucket), I(_I), L(_L), result(_result), saveLfirst(_saveLfirst) { }

bool JoinJob::run(){
    unsigned int *chain = NULL, *table = NULL;
    CHECK ( indexRelation(I.getJoinFieldBucket(bucket_num), I.getBucketSize(bucket_num), chain, table) , "indexing of a bucket failed", delete[] chain; delete[] table; return false; )
    CHECK ( probeResults(L.getJoinFieldBucket(bucket_num), L.getRowIdsBucket(bucket_num), I.getJoinFieldBucket(bucket_num), I.getRowIdsBucket(bucket_num), chain, table, L.getBucketSize(bucket_num), result, saveLfirst),
            "probing a bucket for results failed", delete[] chain; delete[] table; return false; )
    delete[] table;
    delete[] chain;
    return true;
}

/* Hist Job */
HistJob::HistJob(const intField *_joinField, unsigned int _start, unsigned int _end, unsigned int *_Hist, unsigned int *_bucket_nums, unsigned int _H1_N)
        : Job(), num_of_buckets((unsigned int) 0x01 << H1_N) , joinField(_joinField), start(_start), end(_end), Hist(_Hist), bucket_nums(_bucket_nums), H1_N(_H1_N) { }


bool HistJob::run() {
    for (unsigned int i = start ; i < end ; i++){
        bucket_nums[i] = H1(joinField[i], H1_N);
        if ( bucket_nums[i] >= num_of_buckets ){ cerr << "Error: H1() false?" << endl; delete[] Hist; delete[] bucket_nums; return false; }  // ERROR CHECK
        Hist[bucket_nums[i]]++;   // each thread has its one Hist
    }
    return true;
}

/* Partition Job */
PartitionJob::PartitionJob(intField *_newJoinField, intField *_oldJoinField, unsigned int *_newRowids, unsigned int *_oldRowids,
                           unsigned int *_nextBucketPos, unsigned int _start, unsigned int _end, unsigned int _num_of_buckets,
                           const unsigned int *_bucket_nums, unsigned int *_Psum, pthread_mutex_t *_bucket_pos_locks)
                           : Job(), newJoinField(_newJoinField), oldJoinField(_oldJoinField), newRowids(_newRowids), oldRowids(_oldRowids),
                           nextBucketPos(_nextBucketPos), start(_start), end(_end), num_of_buckets(_num_of_buckets), bucket_nums(_bucket_nums), Psum(_Psum), bucket_pos_locks(_bucket_pos_locks) {
}

bool PartitionJob::run() {
    unsigned int next_bucket_pos;
    for (unsigned int i = start ; i < end ; i++) {
        CHECK_PERROR(pthread_mutex_lock(&bucket_pos_locks[bucket_nums[i]]), "pthread_mutex_lock failed", )
        next_bucket_pos = nextBucketPos[bucket_nums[i]];   // accessing this
        nextBucketPos[bucket_nums[i]]++;                   // and increment must be atomic
        CHECK_PERROR(pthread_mutex_unlock(&bucket_pos_locks[bucket_nums[i]]), "pthread_mutex_unlock failed", )
        const unsigned int pos = Psum[bucket_nums[i]] + next_bucket_pos;   // value's position in the re-ordered version
        newJoinField[pos] = oldJoinField[i];
        newRowids[pos] = oldRowids[i];
    }
    return true;
}


#ifndef PARALLEL_IMPLEMENTATION_OF_PARTITION_INSTEAD_OF_THEIR_CALL
void *thread_partition(void *args){
    struct partition_args *argptr = (struct partition_args *) args;
    CHECK( argptr->R.partitionRelationSequentially(argptr->H1_N), "thread partitioning sequentially failed", )
    pthread_exit((void *) 0);
}
#endif
