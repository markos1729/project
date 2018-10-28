#include "Headers/RadixHashJoin.h"
#include <stdio.h>

#define try(a) fprintf(stdout,"\n%s\n",#a);a();
#define assert(a) if (!(a)) fprintf(stderr,"assert %s failed\n",#a);

void test_partition(){
    intField joinField[7] = {'a', 'b', 'a', 'a', 'c', 'b', 'c'};
    unsigned int rowids[7] = {1, 2, 3, 4, 5, 6, 7};
    Relation *R = new Relation(7, joinField, rowids);
    assert( R->partitionRelation(0, 2) )   // forced H1_N = 2
    R->printDebugInfo();
    delete R;
}

void test_index() {
	int *chain,*table;
	intField bucket[10]={3,1,17,23,12,127,123,2,3,10};
	
	assert(indexRelation(bucket,10,chain,table));	
	assert(table[0]==9);
	assert(chain[1]==0);
	assert(chain[2]==0);
	assert(chain[3]==3);
	
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

void test_join(){
    intField joinFieldR[] = {1, 2, 3, 4};
    unsigned int rowidsR[] = {1, 2, 3, 4};
    Relation *R = new Relation(4, joinFieldR, rowidsR);
    intField joinFieldS[] = {1, 1, 3};
    unsigned int rowidsS[] = {1, 2, 3};
    Relation *S = new Relation(3, joinFieldS, rowidsS);
    Result *result = radixHashJoin(*R, *S);
    if (result == NULL){
        cerr << "RadixHashJoined failed!" << endl;
    } else{
        cout << "join results are:" << endl;
        result->printRowIds();
        delete result;
    }
}

int main() {
	try(test_partition);
	try(test_index);
	try(test_probing);
    try(test_join);
	return 0;
}

