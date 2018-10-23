#include "../Headers/RadixHashJoin.h"

/* Local Functions */
bool partitionRelation(Relation &R, intField *&Psum);
bool indexRelation(intField *bucketJoinField, int bucketSize, unsigned int *&chain, unsigned int *&H2HashTable);
bool probeResults(intField *LbucketJoinField, unsigned int *LbucketRowIds, intField *IbucketJoinField, unsigned int *IbucketRowIds, const unsigned int *&chain, const unsigned int *&H2HashTable, int bucketSize, Result &result);


Result *radixHashJoin(Relation &R, Relation &S) {
    // main Pseudocode
    // 0. Define a Result object to fill
    // 1. partition R and S (in place): keep 'Psum' for each bucket in R and S (phase 1)
    // 2. choose one of them for indexing, lets say I, and keep the other foc scanning, lets say L
    // for i in range(0, num_of_buckets):
    //      3. index the bucket[i] of Relation I (phase 2) (create "chain" and fill "bucket")
    //      4. scan joinField of L:
    //              use "bucket" (->change_that_name) and "chain" to find equal joinField values
    //              add rowids of equal joinFields to Result
}

/* Local Function Implementation */
// phase 1: partition in place Relation R into buckets and fill Psum to distinguish them (|Psum| = 2^n)
bool partitionRelation(Relation &R, unsigned int *&Psum){
    //TODO
}

// phase 2: index I's given bucket by creating 'H2HashTable' and 'chain' structures
bool indexRelation(intField *bucketJoinField, int bucketSize, unsigned int *&chain, unsigned int *&H2HashTable){
    //TODO
}

// phase 3: probe linearly L's given bucket and add results based on equal values in indexed I's given bucket
bool probeResults(intField *LbucketJoinField, unsigned int *LbucketRowIds,
                  intField *IbucketJoinField, unsigned int *IbucketRowIds,
                  const unsigned int *&chain, const unsigned int *&H2HashTable,
                  int bucketSize, Result &result){
    //TODO
}
