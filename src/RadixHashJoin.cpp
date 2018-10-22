#include "../Headers/RadixHashJoin.h"

/* Local Functions */
bool partitionRelation(Relation &R, uint32_t *&Psum);
bool indexRelation(uint32_t *bucketJoinField, int bucketSize, uint32_t *&chain, uint32_t *&H2HashTable);
bool probeResults(uint32_t *LbucketJoinField, uint32_t *LbucketRowIds, uint32_t *IbucketJoinField, uint32_t *IbucketRowIds, const uint32_t *&chain, const uint32_t *&H2HashTable, int bucketSize, Result &result);


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
bool partitionRelation(Relation &R, uint32_t *&Psum){
    //TODO
}

// phase 2: index I's given bucket by creating 'H2HashTable' and 'chain' structures
bool indexRelation(uint32_t *bucketJoinField, int bucketSize, uint32_t *&chain, uint32_t *&H2HashTable){
    //TODO
}

// phase 3: probe linearly L's given bucket and add results based on equal values in indexed I's given bucket
bool probeResults(uint32_t *LbucketJoinField, uint32_t *LbucketRowIds,
                  uint32_t *IbucketJoinField, uint32_t *IbucketRowIds,
                  const uint32_t *&chain, const uint32_t *&H2HashTable,
                  int bucketSize, Result &result){
    //TODO
}
