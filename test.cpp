#include <stdio.h>
#include "Headers/RadixHashJoin.h"
#include "Headers/catch.hpp"


TEST_CASE("Trivial partition check", "[partition]") {
    Relation R(0, NULL, NULL);
    REQUIRE( R.partitionRelation(2) );   // H1_N = 2
}

TEST_CASE("Simple case partition check", "[partition]") {
    intField joinField[7] = {'a', 'b', 'a', 'a', 'c', 'b', 'c'};
    unsigned int rowids[7] = {1, 2, 3, 4, 5, 6, 7};
    Relation *R = new Relation(7, joinField, rowids);
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


TEST_CASE("Index is created", "[index]") {
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

TEST_CASE("Results are being created", "[results]") {
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
    unsigned int *H2HashTable = new unsigned int[H2SIZE]();
    H2HashTable[test_value % H2SIZE] = 7;

    Result *result = new Result();
    REQUIRE( probeResults(LbucketjoinField, Lbucketrowids, IbucketjoinField, Ibucketrowids, chain, H2HashTable, 20, result, true) );
    // TODO: add CHECKs here
    result->printRowIds();

    delete[] chain;
    delete[] H2HashTable;
    delete result;
}

SCENARIO("The entire join is being tested", "[RHJ]") {
    GIVEN("The assignment's example R and S") {
        intField joinFieldR[] = {1, 2, 3, 4};
        unsigned int rowidsR[] = {1, 2, 3, 4};
        Relation *R = new Relation(4, joinFieldR, rowidsR);
        intField joinFieldS[] = {1, 1, 3};
        unsigned int rowidsS[] = {1, 2, 3};
        Relation *S = new Relation(3, joinFieldS, rowidsS);
        Result *result = radixHashJoin(*R, *S);

        REQUIRE( result != NULL );
        // TODO: add CHECKs here
        cout << "Join results are:" << endl;
        result->printRowIds();
        delete result;
    }
}
