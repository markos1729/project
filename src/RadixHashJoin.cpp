#include <iostream>
#include <cstring>
#include "../Headers/RadixHashJoin.h"

using namespace std;

#define CHECK(call, msg, action) { if ( ! (call) ) { cerr << msg << endl; action } }

/* Local Functions */
// TODO: readd them here instead of RadixHashJoin.h ?

Result* radixHashJoin(Relation &R, Relation &S) {
    // main Pseudocode
    // 1. partition R and S (in place): keep 'Psum' for each bucket in R and S (phase 1)
    CHECK( R.partitionRelation() , "partitioning R failed", return NULL; )
    CHECK( S.partitionRelation() , "partitioning S failed", return NULL; )

    // 1.5. Define a Result object to fill
    Result *result = new Result;
    
    // 2. choose one of them for indexing, lets say I, and keep the other for scanning, lets say L
    // for i in range(0, num_of_buckets):
    //      3. index the bucket[i] of Relation I (phase 2) (create "chain" and fill "bucket")
    //      4. scan joinField of L:
    //              use "H2HashTable" and "chain" to find equal joinField values
    //              add rowids of equal joinFields to Result
	int nbuckets=R.getBuckets();
	for (int i=0; i<nbuckets; ++i) {
		int *chain,*table;
		indexRelation(R.getField(i),R.getSize(i),chain,table);
		probeResults(R.getField(i),R.getIds(i),S.getField(i),S.getIds(i),chain,table,S.getSize(i),result);
		delete[] chain;
		delete[] table;
	}

    return result;
}

inline int h2(intField key) { return key%H2SIZE; }

/* Local Function Implementation */
// phase 2: index I's given bucket by creating 'H2HashTable' and 'chain' structures
bool indexRelation(intField *bucketJoinField,unsigned int bucketSize,int *&chain,int *&table){
	table=new int[H2SIZE];
	chain=new int[bucketSize];
	int *last=new int[H2SIZE];
	memset(table,-1,H2SIZE*sizeof(int));
	memset(chain,-1,bucketSize*sizeof(int));
	
	for (int i=bucketSize-1; i>=0; --i) {
		int h=h2(bucketJoinField[i]);
		if (table[h]==-1) last[h]=table[h]=i;
		else last[h]=chain[last[h]]=i;
	}
	
	delete[] last;
	return true;
}

// phase 3: probe linearly L's given bucket and add results based on equal values in indexed I's given bucket
// TODO: const parameters
bool probeResults(intField *LbucketJoinField, unsigned int *LbucketRowIds,
                  intField *IbucketJoinField, unsigned int *IbucketRowIds,
                  int *&chain,int *&H2HashTable,
                  unsigned int bucketSize, Result *result){
	for (int i = 0; i < bucketSize; i++) {
		int h = h2(LbucketJoinField[i]);
		if (H2HashTable[h] == -1) continue;		// no records in I with that value
		int chainIndex = H2HashTable[h];
		while (chainIndex > 0) {
			if (LbucketJoinField[i] == IbucketJoinField[chainIndex - 1]) {
				result->addTuple(LbucketRowIds[i], IbucketRowIds[chainIndex - 1]);
			}
			chainIndex = chain[chainIndex - 1];
		}
	}
	return true;
}
