#include <iostream>
#include <cstring>
#include <cmath>
#include <pthread.h>
#include "../Headers/RadixHashJoin.h"

#define CHECK(call, msg, action) { if ( ! (call) ) { std::cerr << msg << std::endl; action } }
#define CHECK_PERROR(call, msg, actions) { if ( (call) < 0 ) { perror(msg); actions } }
#define MAX(A, B) ( (A) > (B) ? (A) : (B) )


using namespace std;


extern JobScheduler *scheduler;


unsigned int H1_N, H2_N;
unsigned int H2(intField value) { return ( value & ( ((1 << (H1_N + H2_N)) - 1) ^ ( (1 << H1_N) - 1) ) ) >> H1_N; }


//for unit testing
void setH(unsigned int _H1_N, unsigned int _H2_N) { H1_N = _H1_N; H2_N = _H2_N; }


Result* radixHashJoin(JoinRelation &R, JoinRelation &S) {
    CHECK( scheduler != NULL, "JobScheduler is not initiated: cannot run parallel RHJ", return NULL; )
    // Partition R and S, whilst keeping a 'Psum' table for each bucket in R and S (phase 1)
    H1_N = (unsigned int) ( ceil( log2( MAX(R.getSize(), S.getSize()) / CACHE ))); // H1_N is the same for both Relations rounded up  TODO: I think ceil() does not work properly!
    H2_N = H1_N/2;
    CHECK( R.partitionRelation(H1_N) , "partitioning R failed", return NULL; )
    CHECK( S.partitionRelation(H1_N) , "partitioning S failed", return NULL; )
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
        unsigned int h = H2(bucketJoinField[i - 1]);
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
		unsigned int h = H2(LbucketJoinField[i]);
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

/* Parallel Job Implementation */
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


HistJob::HistJob(const intField *_joinField, unsigned int _start, unsigned int _end, unsigned int *_Hist, unsigned int *_bucket_nums, unsigned int _H1_N)
        : Job(), num_of_buckets((unsigned int) 0x01 << H1_N) , joinField(_joinField), start(_start), end(_end), Hist(_Hist), bucket_nums(_bucket_nums), H1_N(_H1_N) {
    Hist_bucket_locks = new pthread_mutex_t[num_of_buckets];
    for (unsigned int i = 0 ; i < num_of_buckets ; i++){
        CHECK_PERROR(pthread_mutex_init(&Hist_bucket_locks[i], NULL) , "pthread_mutex_init failed", )
    }
}

HistJob::~HistJob() {
    for (unsigned int i = 0 ; i < num_of_buckets ; i++){
        CHECK_PERROR(pthread_mutex_destroy(&Hist_bucket_locks[i]) , "pthread_mutex_init failed", )
    }
    delete[] Hist_bucket_locks;
}

bool HistJob::run() {
    for (unsigned int i = start ; i < end ; i++){
        bucket_nums[i] = H1(joinField[i], H1_N);
        if ( bucket_nums[i] >= num_of_buckets ){ cerr << "Error: H1() false?" << endl; delete[] Hist; delete[] bucket_nums; return false; }  // ERROR CHECK
        CHECK_PERROR(pthread_mutex_lock(&Hist_bucket_locks[bucket_nums[i]]), "pthread_mutex_lock failed", )
        Hist[bucket_nums[i]]++;   // this must be atomic to work properly
        CHECK_PERROR(pthread_mutex_unlock(&Hist_bucket_locks[bucket_nums[i]]), "pthread_mutex_unlock failed", )
    }
    return true;
}


PartitionJob::PartitionJob(intField *_newJoinField, intField *_oldJoinField, unsigned int *_newRowids, unsigned int *_oldRowids,
                           unsigned int *_nextBucketPos, unsigned int _start, unsigned int _end, unsigned int _num_of_buckets,
                           const unsigned int *_bucket_nums, unsigned int *_Psum)
                           : Job(), newJoinField(_newJoinField), oldJoinField(_oldJoinField), newRowids(_newRowids), oldRowids(_oldRowids),
                           nextBucketPos(_nextBucketPos), start(_start), end(_end), num_of_buckets(_num_of_buckets), bucket_nums(_bucket_nums), Psum(_Psum) {
    bucket_pos_locks = new pthread_mutex_t[num_of_buckets];
    for (unsigned int i = 0 ; i < num_of_buckets ; i++){
        CHECK_PERROR(pthread_mutex_init(&bucket_pos_locks[i], NULL) , "pthread_mutex_init failed", )
    }
}

PartitionJob::~PartitionJob() {
    for (unsigned int i = 0 ; i < num_of_buckets ; i++){
        CHECK_PERROR(pthread_mutex_destroy(&bucket_pos_locks[i]) , "pthread_mutex_init failed", )
    }
    delete[] bucket_pos_locks;
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
#ifdef DDEBUG
        if (newRowids[pos] != 0 ){
            cerr << "Thread Warning: overwriting newRowids[ " << pos << "] = " << newRowids[pos] << " to " << oldRowids[i] << " | next_bucket_pos = " << next_bucket_pos << endl;
        }
#endif
        newRowids[pos] = oldRowids[i];
    }
    return true;
}
