#include "Headers/RadixHashJoin.h"
#include <stdio.h>

#include <gtest/gtest.h>


TEST(partitionRelation, partitionRelation_Relation_test){
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
    ASSERT_TRUE( R->partitionRelation() );
    R->printDebugInfo();
    delete R;
}


TEST(indexRelation, indexRelation_index_Test) {
	int *chain,*table;
	intField bucket[10]={3,1,17,23,12,127,123,2,3,10};
	
	ASSERT_TRUE(indexRelation(bucket,10,chain,table));
    ASSERT_TRUE(table[0]==8);
    ASSERT_TRUE(chain[1]==-1);
    ASSERT_TRUE(chain[2]==-1);
    ASSERT_TRUE(chain[3]==2);
	
	delete[] chain;
	delete[] table;
}

int main(int argc, char *argv[]) {
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
