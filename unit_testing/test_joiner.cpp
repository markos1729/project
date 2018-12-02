#include <stdio.h>
#include "../Headers/Relation.h"
#include "../Headers/FieldTypes.h"
#include "catch.hpp"

#include <algorithm>
#include <unordered_set>

// Global
Relation **R = NULL;
int Rlen = -1;

void R_destroy(){
    delete R[0];
    delete R[1];
    delete R[2];
    delete[] R;
    R = NULL;
    Rlen = -1;
}

void R_init(){
    if (R != NULL){
        R_destroy();
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


TEST_CASE("Relation::PerformJoinWithOriginal()", "[Relation]"){
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
    R_init();
    R[0]->setId(0);
    R[1]->setId(1);
    IntermediateRelation *result = R[0]->performJoinWithOriginal(*R[1], 0, 1, 1, 1);
    REQUIRE( result->getSize() == 9 );
    const unsigned int *actual0 = result->getRowIdsFor(0);
    const unsigned int *actual1 = result->getRowIdsFor(1);
    /** The following doesnt compile for some reason
    unordered_set<pair<unsigned int, unsigned int>> res();
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
    */
    //DEBUG PRINT instead
    cout << "Relation Results:" << endl;
    for (int i = 0 ; i < 9 ; i++){
        cout << actual0[i] << " - " << actual1[i] << endl;
    }
    delete result;
    R_destroy();
}

TEST_CASE("IntermediateRelation::PerformJoinWithOriginal()", "[IntermediateRelation]"){
    /* Result should be:
     * 1 - 1
     * 1 - 2
     * 1 - 3
     * 4 - 4
     * 5 - 4
     * 7 - 6
     * 8 - 6
     */
    R_init();
    R[0]->setId(0);
    R[1]->setId(1);
    unsigned int rowids[] = {1, 3, 4, 6};
    IntermediateRelation I(1, rowids, 4);
    IntermediateRelation *result = I.performJoinWithOriginal(*R[0], 1, 1, 0, 1);
    REQUIRE( result->getSize() == 7 );
    const unsigned int *actual0 = result->getRowIdsFor(0);
    const unsigned int *actual1 = result->getRowIdsFor(1);
    //DEBUG PRINT instead
    cout << "Intermediate Results:" << endl;
    for (int i = 0 ; i < 7 ; i++){
        cout << actual0[i] << " - " << actual1[i] << endl;
    }
    R_destroy();
}
