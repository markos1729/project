#include <algorithm>
#include <cstdio>
#include <utility>
#include <set>

#include "catch.hpp"
#include "../Headers/Relation.h"
#include "../Headers/FieldTypes.h"

using namespace std;

// Global
Relation **R = NULL;
int Rlen = -1;

void R_destroy1(){
    delete R[0];
    delete R[1];
    delete R[2];
    delete[] R;
    R = NULL;
    Rlen = -1;
}

void R_init1(){
    if (R != NULL){
        R_destroy1();
    }
    R = new Relation*[3];
    Rlen = 3;
    {   ////////////////////////////
        R[0] = new Relation(10, 2);
        intField col1[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        intField col2[] = {5, 5, 5, 8, 8, 12, 16, 16, 32, 32};
        R[0]->addColumn(0, col1);
        R[0]->addColumn(1, col2);
    }
    {   ////////////////////////////
        R[1] = new Relation(6, 2);
        intField col1[] = {1, 2, 3, 4, 5, 6};
        intField col2[] = {5, 7, 7, 8, 8, 16};
        R[1]->addColumn(0, col1);
        R[1]->addColumn(1, col2);
    }
    {   ////////////////////////////
        R[2] = new Relation(12, 3);
        intField col1[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
        intField col2[] = {2, 2, 4, 4, 8, 8, 16, 16, 32, 32, 64, 64};
        intField col3[] = {2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096};
        R[2]->addColumn(0, col1);
        R[2]->addColumn(1, col2);
        R[2]->addColumn(2, col3);
    }
}

void (*R_destroy2)() = R_destroy1;

void R_init2(){
    if (R != NULL){
        R_destroy2();
    }
    R = new Relation*[3];
    Rlen = 3;
    {   ////////////////////////////
        R[0] = new Relation(2, 2);
        intField col1[] = {1, 2};
        intField col2[] = {42, 43};
        R[0]->addColumn(0, col1);
        R[0]->addColumn(1, col2);
    }
    {   ////////////////////////////
        R[1] = new Relation(3, 2);
        intField col1[] = {7, 8, 9};
        intField col2[] = {128, 256, 512};
        R[1]->addColumn(0, col1);
        R[1]->addColumn(1, col2);
    }
    {   ////////////////////////////
        R[2] = new Relation(4, 3);
        intField col1[] = {1, 2, 3, 4};
        intField col2[] = {20, 40, 60, 80};
        intField col3[] = {333, 555, 777, 999};
        R[2]->addColumn(0, col1);
        R[2]->addColumn(1, col2);
        R[2]->addColumn(2, col3);
    }
}


TEST_CASE("Relation::performJoinWithOriginal()", "[Relation]"){
    /* Result should be: (size = 9)
     * R[0]     R[1]  --- in rowids -->
     * 1, 5     1, 5                    1 - 1
     * 2, 5     1, 5                    2 - 1
     * 3, 5     1, 5                    3 - 1
     * 4, 8     4, 8                    4 - 4
     * 5, 8     4, 8                    5 - 4
     * 4, 8     5, 8                    4 - 5
     * 5, 8     5, 8                    5 - 5
     * 7, 16    6, 16                   7 - 6
     * 8, 16    6, 16                   8 - 6
     */
    R_init1();
    R[0]->setId(0);
    R[1]->setId(1);
    IntermediateRelation *result = R[0]->performJoinWithOriginal(*R[1], 0, 1, 1, 1);
    REQUIRE( result->getSize() == 9 );
    const unsigned int *actual0 = result->getRowIdsFor(0);
    const unsigned int *actual1 = result->getRowIdsFor(1);
    set<pair<unsigned int, unsigned int>> res;
    for (int i = 0 ; i < 9 ; i++){
        res.insert(make_pair(actual0[i], actual1[i]));
    }
    CHECK( res.find(make_pair(1, 1)) != res.end() );
    CHECK( res.find(make_pair(2, 1)) != res.end() );
    CHECK( res.find(make_pair(3, 1)) != res.end() );
    CHECK( res.find(make_pair(4, 4)) != res.end() );
    CHECK( res.find(make_pair(5, 4)) != res.end() );
    CHECK( res.find(make_pair(4, 5)) != res.end() );
    CHECK( res.find(make_pair(5, 5)) != res.end() );
    CHECK( res.find(make_pair(7, 6)) != res.end() );
    CHECK( res.find(make_pair(8, 6)) != res.end() );
    delete result;
    R_destroy1();
}

TEST_CASE("IntermediateRelation::performJoinWithOriginal()", "[IntermediateRelation]"){
    /* Result should be:
     * I(1) - R[0]
     * 1 - 1
     * 1 - 2
     * 1 - 3
     * 4 - 4
     * 4 - 5
     * 6 - 7
     * 6 - 8
     */
    R_init1();
    R[0]->setId(0);
    R[1]->setId(1);
    unsigned int rowids[] = {1, 3, 4, 6};
    IntermediateRelation I(1, rowids, 4, R[1]);
    IntermediateRelation *result = I.performJoinWithOriginal(*R[0], 1, 1, 0, 1);
    REQUIRE( result->getSize() == 7 );
    const unsigned int *actual0 = result->getRowIdsFor(0);
    const unsigned int *actual1 = result->getRowIdsFor(1);
    set<pair<unsigned int, unsigned int>> res;
    for (int i = 0 ; i < 7 ; i++){
        res.insert(make_pair(actual1[i], actual0[i]));
    }
    CHECK( res.find(make_pair(1, 1)) != res.end() );
    CHECK( res.find(make_pair(1, 2)) != res.end() );
    CHECK( res.find(make_pair(1, 3)) != res.end() );
    CHECK( res.find(make_pair(4, 4)) != res.end() );
    CHECK( res.find(make_pair(4, 5)) != res.end() );
    CHECK( res.find(make_pair(6, 7)) != res.end() );
    CHECK( res.find(make_pair(6, 8)) != res.end() );
    R_destroy1();
}


TEST_CASE("Relation::performCrossProductWithOriginal()", "[Relation]"){
    /* Result should be: (size = 6)
     * 1 - 1
     * 2 - 1
     * 1 - 2
     * 2 - 2
     * 1 - 3
     * 2 - 3
     */
    R_init2();
    R[0]->setId(0);
    R[1]->setId(1);
    IntermediateRelation *result = R[0]->performCrossProductWithOriginal(*R[1]);
    unsigned int resultSize = 6;
    REQUIRE( result->getSize() == resultSize );
    const unsigned int *actual0 = result->getRowIdsFor(0);
    const unsigned int *actual1 = result->getRowIdsFor(1);
    set<pair<unsigned int, unsigned int>> res;
    for (int i = 0 ; i < resultSize ; i++){
        res.insert(make_pair(actual0[i], actual1[i]));
    }
    CHECK( res.find(make_pair(1, 1)) != res.end() );
    CHECK( res.find(make_pair(2, 1)) != res.end() );
    CHECK( res.find(make_pair(1, 2)) != res.end() );
    CHECK( res.find(make_pair(2, 2)) != res.end() );
    CHECK( res.find(make_pair(1, 3)) != res.end() );
    CHECK( res.find(make_pair(2, 3)) != res.end() );
    delete result;
    R_destroy2();
}

TEST_CASE("IntermediateRelation::performCrossProductWithOriginal()", "[IntermediateRelation]"){
    /* Result should be: (size = 8)
     * 1 - 4
     * 1 - 6
     * 1 - 8
     * 1 - 9
     * 2 - 4
     * 2 - 6
     * 2 - 8
     * 2 - 9
     */
    R_init2();
    R[0]->setId(0);
    unsigned int rowids[] = {4, 6, 8, 9};
    IntermediateRelation I(1, rowids, 4, R[1]);
    IntermediateRelation *result = I.performCrossProductWithOriginal(*R[0]);
    const unsigned int resultSize = 8;
    REQUIRE( result->getSize() == resultSize );
    const unsigned int *actual0 = result->getRowIdsFor(0);
    const unsigned int *actual1 = result->getRowIdsFor(1);
    set<pair<unsigned int, unsigned int>> res;
    for (int i = 0 ; i < resultSize ; i++){
        res.insert(make_pair(actual0[i], actual1[i]));
    }
    CHECK( res.find(make_pair(1, 4)) != res.end() );
    CHECK( res.find(make_pair(1, 6)) != res.end() );
    CHECK( res.find(make_pair(1, 8)) != res.end() );
    CHECK( res.find(make_pair(1, 9)) != res.end() );
    CHECK( res.find(make_pair(2, 4)) != res.end() );
    CHECK( res.find(make_pair(2, 6)) != res.end() );
    CHECK( res.find(make_pair(2, 8)) != res.end() );
    CHECK( res.find(make_pair(2, 9)) != res.end() );
    R_destroy2();
}
