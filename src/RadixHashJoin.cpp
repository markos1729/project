#include <iostream>
#include "../Headers/RadixHashJoin.h"

using namespace std;

#define CHECK(call, msg, action) { if ( ! (call) ) { cerr << msg << endl; action } }

/* Local Functions */
bool indexRelation(intField *bucketJoinField, int bucketSize, unsigned int *&chain, unsigned int *&H2HashTable);
bool probeResults(intField *LbucketJoinField, unsigned int *LbucketRowIds, intField *IbucketJoinField, unsigned int *IbucketRowIds, const unsigned int *&chain, const unsigned int *&H2HashTable, int bucketSize, Result &result);


Result *radixHashJoin(Relation &R, Relation &S) {
    // main Pseudocode
    // 1. partition R and S (in place): keep 'Psum' for each bucket in R and S (phase 1)
    CHECK( R.partitionRelation() , "partitioning R failed", return NULL; )
    CHECK( S.partitionRelation() , "partitioning S failed", return NULL; )
    // 1.5. Define a Result object to fill
    Result *result = new Result;
    // 2. choose one of them for indexing, lets say I, and keep the other foc scanning, lets say L
    // for i in range(0, num_of_buckets):
    //      3. index the bucket[i] of Relation I (phase 2) (create "chain" and fill "bucket")
    //      4. scan joinField of L:
    //              use "bucket" (->change_that_name) and "chain" to find equal joinField values
    //              add rowids of equal joinFields to Result
    return result;
}

/* Local Function Implementation */
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
