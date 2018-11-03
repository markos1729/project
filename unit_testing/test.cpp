#include <stdio.h>
#include "catch.hpp"
#include "../Headers/RadixHashJoin.h"

// The following are only used for unit_testing, not for the implementation of Radix Hash Join
#include <vector>
#include <algorithm>


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




//TODO: add trivial case check for Indexing

TEST_CASE("Simple case Indexing check", "[index]") {
    unsigned int *chain, *table;
    intField bucket[10] = {3, 1, 17, 23, 12, 127, 123, 2, 3, 10};

    REQUIRE(indexRelation(bucket, 10, chain, table));
    CHECK(table[0] == 9);
    CHECK(chain[1] == 0);
    CHECK(chain[2] == 0);
    CHECK(chain[3] == 3);

    delete[] chain;
    delete[] table;
}

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
        // TODO: add CHECKs here
        cout << "Join results are:" << endl;
        result->printRowIds();
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
            
            JoinRelation JR=*R.extractJoinRelation(1);
            JoinRelation JS=*S.extractJoinRelation(1);

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

            sort(found.begin(),found.end());
            sort(expected.begin(),expected.end());
            
            CHECK( found == expected );
        }
        catch (...) { printf("Could not load relations\n"); }
    }
}