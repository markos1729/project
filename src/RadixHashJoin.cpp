#include <iostream>
#include <cstring>
#include <cmath>
#include "../Headers/RadixHashJoin.h"

using namespace std;

#define CHECK(call, msg, action) { if ( ! (call) ) { cerr << msg << endl; action } }
#define MAX(A, B) ( (A) > (B) ? (A) : (B) )

/* Local Functions */
// TODO: read them here instead of RadixHashJoin.h ?

/* *** Hash Functions Example ***

...0000000001111 (take last 4 bits for H1) = value & ((1<<H1_N)-1)
...0000000110000 (take the next 4/2=2 bits for H2) = value & (((1<<H2_N)-1)^((1<<H1_N)-1))
        
...0000000111111 \
		^		  -> ......0000000110000 !!!
...0000000001111 /

   ****************************** */

unsigned int H1_N,H2_N;
unsigned int H2_OLD_MAY_REPLACE_H1_SEE_EXPLANATION(intField value) { return value & ((1 << H2_N) - 1); }
unsigned int H2(intField value) { return ( value & ( ((1 << (H1_N + H2_N)) - 1) ^ ( (1 << H1_N) - 1) ) ) >> H1_N; }


//for unit testing
void setH(unsigned int _H1_N,unsigned int _H2_N) { H1_N=_H1_N; H2_N=_H2_N; }

Result* radixHashJoin(JoinRelation &R, JoinRelation &S) {
    // Partition R and S, whilst keeping a 'Psum' table for each bucket in R and S (phase 1)
    H1_N = (unsigned int) ( ceil( log2( MAX(R.getSize(), S.getSize()) / CACHE ))); // H1_N is the same for both Relations rounded up
    H2_N = H1_N/2;
    CHECK( R.partitionRelation(H1_N) , "partitioning R failed", return NULL; )
    CHECK( S.partitionRelation(H1_N) , "partitioning S failed", return NULL; )
    // Define a Result object to fill
    Result *result = new Result;
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
    
    // Iteratively perform phases 2 and 3 for all buckets of both similarly partitioned Relations and add results bucket by bucket
	for (unsigned int i = 0 ; i < I->getNumberOfBuckets() ; i++) {
		unsigned int *chain = NULL, *table = NULL;
		CHECK ( indexRelation(I->getJoinFieldBucket(i), I->getBucketSize(i), chain, table) , "indexing of a bucket failed", delete[] chain; delete[] table; delete result; return NULL; )
		CHECK ( probeResults(L->getJoinFieldBucket(i), L->getRowIdsBucket(i), I->getJoinFieldBucket(i), I->getRowIdsBucket(i), chain, table, L->getBucketSize(i), result, saveLfirst) , "probing a bucket for results failed", delete[] chain; delete[] table; delete result; return NULL; )
		delete[] table;
		delete[] chain;
	}
    return result;
}

/* Local Function Implementation */
// phase 2: index I's given bucket by creating 'H2HashTable' and 'chain' structures
bool indexRelation(intField *bucketJoinField, unsigned int bucketSize, unsigned int *&chain, unsigned int *&table){
	unsigned int sz = 1 << H2_N;
    table = new unsigned int[sz];
    chain = new unsigned int[bucketSize];
    unsigned int *last = new unsigned int[sz];
    
    memset(table, 0, sz * sizeof(unsigned int));
    memset(chain, 0, bucketSize * sizeof(unsigned int));
    for (unsigned int i = bucketSize ; i > 0 ; --i) {
        unsigned int h = H2(bucketJoinField[i - 1]);
    //    printf("%d %d\n",i,h);
        if (table[h] == 0) last[h] = table[h] = i;
        else last[h] = chain[last[h] - 1] = i;
    }
    
  //  for (int i=0; i<sz; i++) printf("%d ",table[i]); printf("\n");
//    for (int i=0; i<bucketSize; i++) printf("%d\n",chain[i]);
    
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
