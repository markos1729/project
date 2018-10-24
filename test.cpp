#include "Headers/RadixHashJoin.h"
#include <stdio.h>


#define try(a) fprintf(stdout,"%s\n",#a);a();
#define assert(a) if (!(a)) fprintf(stderr,"assert %s failed\n",#a);

void test_index() {
	assert(0==1);
	assert(2==2);
	assert(3==2);
}

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

int main() {
	try(test_partition);
	return 0;
}
