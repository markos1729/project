#include <iostream>
#include "../Headers/RadixHashJoin.h"
#include "../Headers/HashFunctions.h"
#include "../Headers/JoinRelation.h"
#include "../Headers/macros.h"
#include "../Headers/ConfigureParameters.h"


extern JobScheduler *scheduler;


/* Join Relation Implementation */
JoinRelation::JoinRelation(unsigned int _size, const intField *_joinField, const unsigned int *_rowids) : size(_size), joinField(NULL), rowids(NULL), Psum(NULL), numberOfBuckets(0) {
    joinField = new intField[size];
    rowids = new unsigned int[size];
    for (unsigned int i = 0; i < size; i++) {
        joinField[i] = _joinField[i];
        rowids[i] = _rowids[i];
    }
}

JoinRelation::~JoinRelation() {
    delete[] Psum;
    delete[] joinField;
    delete[] rowids;
}

// (multi-threaded version) phase 1: partition in place JoinRelation R into buckets and fill Psum to distinguish them (|Psum| = num_of_buckets = 2^H1_N)
bool JoinRelation::partitionRelation(unsigned int H1_N) {
    if (this->getSize() == 0 || rowids == NULL || joinField == NULL ) return true;     // nothing to partition
    const unsigned int num_of_buckets = (unsigned int) 0x01 << H1_N;  // = 2^H1_N
    // 1) calculate Hist using available threads
    const unsigned int threads_to_use = scheduler->get_number_of_threads();
    unsigned int *Hist = new unsigned int[num_of_buckets]();          // all Hist[i] are initialized to 0
    unsigned int *bucket_nums = new unsigned int[size];
    unsigned int **tempHists = new unsigned int *[threads_to_use];
    const unsigned int chunk_size = size / threads_to_use + ((size % threads_to_use > 0) ? 1 : 0);
    for (int i = 0 ; i < threads_to_use; i++){
        tempHists[i] = new unsigned int[num_of_buckets]();            // (!) init to 0
        if (i * chunk_size < size ) scheduler->schedule(new HistJob(joinField, i * chunk_size, MIN((i+1) * chunk_size, size), tempHists[i], bucket_nums, H1_N));
    }
    scheduler->waitUntilAllJobsHaveFinished();
    // and then sum them up to create one Hist
    bool failed = false;
    for (unsigned int j = 0 ; j < threads_to_use ; j++){
        if (tempHists[j] == NULL) { failed = true; continue; }
        for (unsigned int i = 0 ; i < num_of_buckets ; i++){
            Hist[i] += tempHists[j][i];
        }
        delete[] tempHists[j];
    }
    delete[] tempHists;
    if (failed) { cerr << "Error: HistJob failed!" << endl; delete[] Hist; delete[] bucket_nums; return false; }   // should not happen
    // 2) convert Hist table to Psum table
    unsigned int sum = 0;
    for (unsigned int i = 0 ; i < num_of_buckets ; i++){
        unsigned int temp = Hist[i];
        Hist[i] = sum;
        sum += temp;
    }
    Psum = Hist;
    numberOfBuckets = num_of_buckets;
    // 3) create new re-ordered versions for joinField and rowids based on their bucket_nums using available threads
    intField *newJoinField = new intField[size]();
    unsigned int *newRowids = new unsigned int[size]();
    unsigned int *nextBucketPos = new unsigned int[num_of_buckets]();
    pthread_mutex_t *bucket_pos_locks = new pthread_mutex_t[num_of_buckets];   // one lock per bucket i to protect nextBocketPos[i]
    for (unsigned int i = 0 ; i < num_of_buckets ; i++){
        CHECK_PERROR(pthread_mutex_init(&bucket_pos_locks[i], NULL) , "pthread_mutex_init failed", )
    }
    for (int i = 0 ; i < threads_to_use ; i++){
        if (i * chunk_size < size ) scheduler->schedule(new PartitionJob(newJoinField, joinField, newRowids, rowids, nextBucketPos, i * chunk_size, MIN((i + 1) * chunk_size, size), num_of_buckets, bucket_nums, Psum, bucket_pos_locks));
    }
    scheduler->waitUntilAllJobsHaveFinished();
    for (unsigned int i = 0 ; i < num_of_buckets ; i++){
        CHECK_PERROR(pthread_mutex_destroy(&bucket_pos_locks[i]) , "pthread_mutex_init failed", )
    }
    delete[] bucket_pos_locks;
    delete[] nextBucketPos;
    delete[] bucket_nums;
    // 4) overwrite joinField and rowids with new re-ordered versions of it
    delete[] joinField;
    joinField = newJoinField;
    delete[] rowids;
    rowids = newRowids;
    return true;
}

// (single-threaded version) phase 1: partition in place JoinRelation R into buckets and fill Psum to distinguish them (|Psum| = num_of_buckets = 2^H1_N)
bool JoinRelation::partitionRelationSequentially(unsigned int H1_N) {
    if (this->getSize() == 0 || rowids == NULL || joinField == NULL ) return true;     // nothing to partition
    const unsigned int num_of_buckets = (unsigned int) 0x01 << H1_N;  // = 2^H1_N
    // 1) calculate Hist - O(n)
    unsigned int *Hist = new unsigned int[num_of_buckets]();    // all Hist[i] are initialized to 0
    unsigned int *bucket_nums = new unsigned int[size];
    for (unsigned int i = 0 ; i < size ; i++){
        bucket_nums[i] = H1(joinField[i], H1_N);
        if ( bucket_nums[i] >= num_of_buckets ){ delete[] Hist; delete[] bucket_nums; return false; }  // ERROR CHECK
        Hist[bucket_nums[i]]++;
    }
    // 2) convert Hist table to Psum table
    unsigned int sum = 0;
    for (unsigned int i = 0 ; i < num_of_buckets ; i++){
        unsigned int temp = Hist[i];
        Hist[i] = sum;
        sum += temp;
    }
    Psum = Hist;
    numberOfBuckets = num_of_buckets;
    // 3) create new re-ordered versions for joinField and rowids based on their bucket_nums - O(n)
    intField *newJoinField = new intField[size]();
    unsigned int *newRowids = new unsigned int[size]();
    unsigned int *nextBucketPos = new unsigned int[num_of_buckets]();
    for (unsigned int i = 0 ; i < size ; i++) {
        const unsigned int pos = Psum[bucket_nums[i]] + nextBucketPos[bucket_nums[i]];   // value's position in the re-ordered version
        if ( pos >= size ) { delete[] newJoinField; delete[] newRowids; delete[] bucket_nums; delete[] nextBucketPos; Psum = NULL; numberOfBuckets = 0;  return false; }  // ERROR CHECK
        newJoinField[pos] = joinField[i];
        newRowids[pos] = rowids[i];
        nextBucketPos[bucket_nums[i]]++;
    }
    delete[] nextBucketPos;
    delete[] bucket_nums;
    // 4) overwrite joinField and rowids with new re-ordered versions of it
    delete[] joinField;
    joinField = newJoinField;
    delete[] rowids;
    rowids = newRowids;
    return true;
}


#ifdef DDEBUG
void JoinRelation::printDebugInfo() {
    if (Psum != NULL) {
        printf("This JoinRelation is partitioned.\n%u buckets created with Psum as follows:\n", numberOfBuckets);
        for (unsigned int i = 0; i < numberOfBuckets; i++) {
            printf("Psum[%u] = %u\n", i, Psum[i]);
        }
    }
    printf(" joinField | rowids\n");
    for (unsigned int i = 0 ; i < size ; i++){
        printf("%10u | %u\n", (unsigned int) joinField[i], rowids[i]);
    }
}
#endif
