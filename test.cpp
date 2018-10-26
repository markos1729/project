#include "Headers/RadixHashJoin.h"
#include <stdio.h>

#define try(a) fprintf(stdout,"\n%s\n",#a);a();
#define assert(a) if (!(a)) fprintf(stderr,"assert %s failed\n",#a);

void test_partition(){
    intField *joinField = new intField[7];
    joinField[0] = 'a';
    joinField[1] = 'b';
    joinField[2] = 'a';
    joinField[3] = 'a';
    joinField[4] = 'c';
    joinField[5] = 'b';
    joinField[6] = 'c';
    unsigned int *rowids = new unsigned int[7];
    for (unsigned int i = 0 ; i < 7 ; i++) { rowids[i] = i+1; }
    Relation *R = new Relation(7, joinField, rowids);
    assert( R->partitionRelation() )
    R->printDebugInfo();
    delete R;
}

void test_index() {
	int *chain,*table;
	intField bucket[10]={3,1,17,23,12,127,123,2,3,10};
	
	assert(indexRelation(bucket,10,chain,table));	
	assert(table[0]==8);
	assert(chain[1]==-1);
	assert(chain[2]==-1);
	assert(chain[3]==2);
	
	delete[] chain;
	delete[] table;
}

void test_probing() {
    intField test_value = 42;
    intField IbucketjoinField[8] = {1, 2, test_value, test_value, 5, 6, test_value, 8};
    unsigned int Ibucketrowids[8];
    for (unsigned int i = 0 ; i < 8 ; i++) { Ibucketrowids[i] = i+1; }

    intField LbucketjoinField[20] = { 0 };
    LbucketjoinField[2] = test_value;
    LbucketjoinField[11] = test_value;
    unsigned int Lbucketrowids[20];
    for (unsigned int i = 0 ; i < 20 ; i++) { Lbucketrowids[i] = i+1; }

    int *chain = new int[8]();
    chain[3] = 3; chain[6] = 4;
    int *H2HashTable = new int[H2SIZE]();
    H2HashTable[test_value % H2SIZE] = 7;

    Result *result = new Result();
    assert( probeResults(LbucketjoinField, Lbucketrowids, IbucketjoinField, Ibucketrowids, chain, H2HashTable, 20, result) );
    // DEBUG: we could have asserts here, but we're gonna use unit testing instead anyway
    result->printRowIds();

    delete[] chain;
    delete[] H2HashTable;
    delete result;
}

int main() {
	try(test_partition);
	try(test_index);
	try(test_probing);
	return 0;
}

