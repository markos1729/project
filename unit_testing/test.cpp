#include <stdio.h>
#include "catch.hpp"
#include "../Headers/RadixHashJoin.h"

// The following are only used for unit_testing, not for the implementation of Radix Hash Join
#include <vector>
#include <algorithm>

/* Partition Unit Testing */
TEST_CASE("Trivial case partition check", "[partition]") {
    JoinRelation R(0, NULL, NULL);
    REQUIRE( R.partitionRelation(2) );   // H1_N = 2
}

TEST_CASE("Simple case partition check", "[partition]") {
    intField joinField[7] = {'a', 'b', 'a', 'a', 'c', 'b', 'c'};
    unsigned int rowids[7] = {1, 2, 3, 4, 5, 6, 7};
    JoinRelation *R = new JoinRelation(7, joinField, rowids);
    REQUIRE( R->partitionRelation(2) );   // H1_N = 2
    CHECK( R->getJoinField(0) == 'a' );
    CHECK( R->getJoinField(1) == 'a' );
    CHECK( R->getJoinField(2) == 'a' );
    CHECK( R->getJoinField(3) == 'b' );
    CHECK( R->getJoinField(4) == 'b' );
    CHECK( R->getJoinField(5) == 'c' );
    CHECK( R->getJoinField(6) == 'c' );
    delete R;
}

TEST_CASE("Realistic case partition check", "[partition]"){
    const char file_r[] = "Files/r7";
    try {
        Relation R(file_r);
        
        JoinRelation *JR = R.extractJoinRelation(1);
        
        unsigned int  H1_N = (unsigned int) ( ceil( log2( JR->getSize() / CACHE )));
        
        //cout << "Realistic case partition check: H1_N = " << H1_N << endl;

        REQUIRE( JR->partitionRelation(H1_N) );

        // check if result is correct using Psum and H1
        unsigned int i = 0,    // index of first element in JR's buckets 
                     j = 0,    // offset of each element of current bucket
                     k = 0;    // number of current bucket
        while ( i <= JR->getSize() ){
            unsigned int cur_bucket_count = JR->getBucketSize(k);
            for (j = 0; j < cur_bucket_count ; j++){
                CHECK( H1(JR->getJoinField(i + j), H1_N) == k );            // make sure each element of the bucket is actually hashed to its assigned bucket
            }
            k++;
            i += cur_bucket_count;
        }

        delete JR;
    }
    catch (...) { printf("Could not load relations\n"); }
}

/* Index Unit Testing */
TEST_CASE("Trivial case Indexing check", "[index]") {
    unsigned int *chain, *table;
    REQUIRE( indexRelation(NULL, 0, chain, table) );
    delete[] chain;
    delete[] table;
}


TEST_CASE("Simple case Indexing check", "[index]") {
    unsigned int *chain, *table;
    intField bucket[10] = {30, 228, 17, 23, 12, 127, 123, 2018, 3094, 10};
	//hash function:        1    2   1   1   0    3    3     2     1   0
	//virtual rowid:        1    2   3   4   5    6    7     8     9  10

	setH(4,2); //set hash bits for testing
    REQUIRE( indexRelation(bucket, 10, chain, table) );
    
    //according to the numbers above   
    CHECK( table[0] == 10 );
    CHECK( table[1] == 9 );
    CHECK( table[2] == 8 );
    CHECK( table[3] == 7 );
    
    //also check the chain numbers
    CHECK( chain[0] == 0 );
    CHECK( chain[1] == 0 );
    CHECK( chain[2] == 1 );
    CHECK( chain[3] == 3 );
    CHECK( chain[4] == 0 );
    CHECK( chain[5] == 0 );
    CHECK( chain[6] == 6 );
    CHECK( chain[7] == 2 );
    CHECK( chain[8] == 4 );
    CHECK( chain[9] == 5 );

    delete[] chain;
    delete[] table;
}

TEST_CASE("Realistic case Indexing check", "[index]") {
    unsigned int *chain, *table;
	intField bucket[1000];
	
	//1000 sized bucket containing 5 distinct values
	for (unsigned int i=0; i<1000; ++i) 
		switch (i%5) {                         // hash
			case 0: bucket[i]=182172; break;   //   17
			case 1: bucket[i]=271; break;      //    0
			case 2: bucket[i]=11381378; break; //   10 
			case 3: bucket[i]=8912441; break;  //   31
			case 4: bucket[i]=5516; break;     //    5
			}

	setH(10,5); //set hash bits for testing
	REQUIRE( indexRelation(bucket, 1000, chain, table) );

	//only these should be in table
	CHECK( table[17] ==  996 );
	CHECK(  table[0] ==  997 );
	CHECK( table[10] ==  998 );
	CHECK( table[31] ==  999 );
	CHECK(  table[5] == 1000 );

	//there should be 27 '0' in table
	unsigned counter=0;
	for (unsigned int i=0; i<32; ++i) if (table[i]==0) counter++;
	CHECK( counter == 27 );

	//the first five values in chain should be '0'
	CHECK( chain[0] == 0 );
	CHECK( chain[1] == 0 );
	CHECK( chain[2] == 0 );
	CHECK( chain[3] == 0 );
	CHECK( chain[4] == 0 );
	
	//the rest should be the numbers 1..995
	CHECK( chain[5] == 1 );
	for (unsigned int i=6; i<1000; ++i) CHECK( chain[i] == chain[i-1]+1 );

    delete[] chain;
    delete[] table;
}


/* Probing Unit Testing */
TEST_CASE("Probing for results", "[probing]") {
    intField test_value = 42;
    intField IbucketjoinField[8] = {1, 2, test_value, test_value, 5, 6, test_value, 8};
    unsigned int Ibucketrowids[8];
    for (unsigned int i = 0 ; i < 8 ; i++) { Ibucketrowids[i] = i+1; }

    intField LbucketjoinField[20] = { 0 };
    LbucketjoinField[2] = test_value;
    LbucketjoinField[11] = test_value;
    unsigned int Lbucketrowids[20];
    for (unsigned int i = 0 ; i < 20 ; i++) { Lbucketrowids[i] = i+1; }

    unsigned int *chain = new unsigned int[8]();
    chain[3] = 3; chain[6] = 4;
    unsigned int *H2HashTable = new unsigned int[CACHE]();
    H2HashTable[test_value % CACHE] = 7;

    Result *result = new Result();
    REQUIRE( probeResults(LbucketjoinField, Lbucketrowids, IbucketjoinField, Ibucketrowids, chain, H2HashTable, 20, result, true) );
    // TODO: add CHECKs here
    result->printRowIds();

    delete[] chain;
    delete[] H2HashTable;
    delete result;
}

/* Radix Hash Join Unit Testing */
SCENARIO("The entire join is being tested on a simple case", "[RHJ]") {
    GIVEN("The assignment's example R and S") {
        intField joinFieldR[] = {1, 2, 3, 4};
        unsigned int rowidsR[] = {1, 2, 3, 4};
        JoinRelation *R = new JoinRelation(4, joinFieldR, rowidsR);
        
        intField joinFieldS[] = {1, 1, 3};
        unsigned int rowidsS[] = {1, 2, 3};
        JoinRelation *S = new JoinRelation(3, joinFieldS, rowidsS);
        
        Result *result = radixHashJoin(*R, *S);

        REQUIRE( result != NULL );
        
        Iterator p(result);
        unsigned int rid = 0, sid = 0, count = 0;
        while ( p.getNext(rid, sid) ){
            count++;
            CHECK ( ((rid == 1 && sid == 1) || (rid == 1 && sid == 2) || (rid == 3 && sid == 3)) );
        }
        CHECK( count == 3 );
        
        delete result;
    }
}

SCENARIO("The entire join is being tested on a realistically-sized case", "[RHJ]") {
    GIVEN("Some input files from SIGMOD's assignment") {
        char file_r[]="Files/r7";
        char file_s[]="Files/r12";
        
        try {
            Relation R(file_r);
            Relation S(file_s);

            vector<pair<unsigned int,unsigned int>> found;
            vector<pair<unsigned int,unsigned int>> expected;
            
            JoinRelation *JRptr = R.extractJoinRelation(1);
            JoinRelation *JSptr = S.extractJoinRelation(1);
            JoinRelation &JR = *JRptr;
            JoinRelation &JS = *JSptr;


            for (unsigned int i=0; i<JR.getSize(); ++i) {
                intField RV=JR.getJoinField(i);
                for (unsigned int j=0; j<JS.getSize(); ++j) {
                    intField RS=JS.getJoinField(j);
                    if (RS==RV) expected.push_back(make_pair(i+1,j+1));
                }   
            }   
            
            unsigned int i,j;
            Result *J=radixHashJoin(JR,JS);
            Iterator I(J);
            while (I.getNext(i,j)) found.push_back(make_pair(i,j));

            delete JRptr;
            delete JSptr;

            sort(found.begin(),found.end());
            sort(expected.begin(),expected.end());
            
            CHECK( found == expected );
        }
        catch (...) { printf("Could not load relations\n"); }
    }
}
