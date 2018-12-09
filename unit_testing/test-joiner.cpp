#include <algorithm>
#include <cstdio>
#include <utility>
#include <set>

#include "catch.hpp"
#include "../Headers/Relation.h"
#include "../Headers/FieldTypes.h"

using namespace std;

// Global
Relation **R = NULL;     // Relations to be used for testing as an example
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


////////////////////////////////////////////
///                FILTER                ///
////////////////////////////////////////////

TEST_CASE("Relation::performFilter() - trivial case", "[FILTER]") {
	R_init1();
    R[0]->setId(0);
    R[1]->setId(1);
    R[2]->setId(2);

    IntermediateRelation *result=R[0]->performFilter(0,1,10,'=');
    REQUIRE(result!=NULL);
    CHECK(result->getSize()==0);
    delete result;
    
    result=R[1]->performFilter(1,0,1,'<');
    REQUIRE(result!=NULL);
    CHECK(result->getSize()==0);
    delete result;

	result=R[2]->performFilter(2,2,4096,'>');
    REQUIRE(result!=NULL);
    CHECK(result->getSize()==0);
    delete result;

    R_destroy1();
}

TEST_CASE("Relation::performFilter()", "[FILTER]") {
    R_init1();
    R[0]->setId(0);
    R[1]->setId(1);
    R[2]->setId(2);
        
    IntermediateRelation *result=R[0]->performFilter(0,1,5,'=');
    REQUIRE(result->getSize()==3);
    unsigned int *actual=result->getRowIdsFor(0);
    CHECK(actual[0]==1);
    CHECK(actual[1]==2);
	CHECK(actual[2]==3);
   	delete result;
   	
   	result=R[1]->performFilter(1,1,5,'>');
    REQUIRE(result->getSize()==5);
    actual=result->getRowIdsFor(1);
    CHECK(actual[0]==2);
    CHECK(actual[1]==3);
	CHECK(actual[2]==4);
	CHECK(actual[3]==5);
	CHECK(actual[4]==6);
   	delete result;
   	
   	result=R[2]->performFilter(2,2,30,'<');
    REQUIRE(result->getSize()==4);
    actual=result->getRowIdsFor(2);
    CHECK(actual[0]==1);
    CHECK(actual[1]==2);
	CHECK(actual[2]==3);
	CHECK(actual[3]==4);
   	delete result;
   	
   	R_destroy1();
}

TEST_CASE("IntermediateRelation::performFilter() - trivial case", "[FILTER]") {
	R_init1();
    R[0]->setId(0);
    R[1]->setId(1);
    R[2]->setId(2);

	unsigned int rowids0[]={1,3,4,6};
    IntermediateRelation I0(0,rowids0,4,R[0]);
    IntermediateRelation *result=I0.performFilter(0,1,16,'=');
    REQUIRE(result!=NULL);
    CHECK(result->getSize()==0);
    
	unsigned int rowids1[]={4,5,6};
    IntermediateRelation I1(1,rowids1,3,R[1]);
    result=I1.performFilter(1,0,2,'<');
    REQUIRE(result!=NULL);
    CHECK(result->getSize()==0);

	unsigned int rowids2[]={2,4,6,7,8};
    IntermediateRelation I2(2,rowids2,5,R[2]);
	result=I2.performFilter(2,2,300,'>');
    REQUIRE(result!=NULL);
    CHECK(result->getSize()==0);
}


TEST_CASE("IntermediateRelation::performFilter()", "[FILTER]") {
    R_init1();
    R[0]->setId(0);
    R[1]->setId(1);
    R[2]->setId(2);
        
	unsigned int rowids0[]={1,3,4,5,6,7,8,9};
    IntermediateRelation I0(0,rowids0,9,R[0]);
    IntermediateRelation *result=I0.performFilter(0,1,5,'=');
    REQUIRE(result->getSize()==2);
    unsigned int *actual=result->getRowIdsFor(0);
    CHECK(actual[0]==1);
    CHECK(actual[1]==3);
   	
	unsigned int rowids1[]={1,4,6};
    IntermediateRelation I1(1,rowids1,3,R[1]);
    result=I1.performFilter(1,0,6,'<');
    REQUIRE(result->getSize()==2);
    actual=result->getRowIdsFor(1);
    CHECK(actual[0]==1);
    CHECK(actual[1]==4);
   	
	unsigned int rowids2[]={4,5,7,9,10,11,12};
    IntermediateRelation I2(2,rowids2,7,R[2]);
	result=I2.performFilter(2,2,127,'>');
    REQUIRE(result->getSize()==5);
    actual=result->getRowIdsFor(2);
    CHECK(actual[0]==7);
    CHECK(actual[1]==9);
    CHECK(actual[2]==10);
	CHECK(actual[3]==11);
	CHECK(actual[4]==12);
   	
   	R_destroy1();
}


////////////////////////////////////////////
///            EQUAL COLUMNS             ///
////////////////////////////////////////////

TEST_CASE("Relation::performEqColumns() - trivial case", "[EQUAL COLUMNS]") {
	R_init1();
	R[0]->setId(0);

	IntermediateRelation *result=R[0]->performEqColumns(0,0,0,1);
	REQUIRE(result!=NULL);
	CHECK(result->getSize()==0);
	delete result;

	R_destroy1();
}

TEST_CASE("Relation::performEqColumns()", "[EQUAL COLUMNS]") {
	R_init1();
	R[2]->setId(2);
	
	IntermediateRelation *result=R[2]->performEqColumns(2,2,0,1);
	unsigned int *actual=result->getRowIdsFor(2);
	REQUIRE(result!=NULL);
	CHECK(result->getSize()==2);
	CHECK(actual[0]==2);
	CHECK(actual[1]==4);
	delete result;

	R_destroy1();
}

TEST_CASE("IntermediateRelation::performEqColumns() - trivial case", "[EQUAL COLUMNS]") {
	R_init1();
	R[0]->setId(0);
	R[2]->setId(2);

	unsigned int rowids0[]={1,2,4,5,6,7,9};
	unsigned int rowids2[]={1,2,3,7,8,9,12};
    IntermediateRelation I(0,2,rowids0,rowids2,7,R[0],R[2]);

	IntermediateRelation *result=I.performEqColumns(0,2,1,1);
	REQUIRE(result!=NULL);
	CHECK(result->getSize()==0);

	R_destroy1();
}

TEST_CASE("IntermediateRelation::performEqColumns()", "[EQUAL COLUMNS]") {
	R_init1();
	R[0]->setId(0);
	R[2]->setId(2);

	unsigned int rowids0[]={1,2,3,4,5,6,7,8,9,10};
	unsigned int rowids2[]={1,2,3,4,5,6,7,8,9,10};
    IntermediateRelation I(0,2,rowids0,rowids2,10,R[0],R[2]);

	IntermediateRelation *result=I.performEqColumns(0,2,0,0);
	REQUIRE(result!=NULL);
	REQUIRE(result->getSize()==10);
	unsigned int *actual=result->getRowIdsFor(0);
	for (unsigned int i=0; i<10; ++i)
		CHECK(actual[i]==i+1);

	unsigned int rowids00[]={1,4,5,6,7,8,9,10};
	unsigned int rowids22[]={3,5,6,12,7,8,9,10};
    IntermediateRelation II(0,2,rowids00,rowids22,10,R[0],R[2]);

	result=II.performEqColumns(0,2,1,1);
	REQUIRE(result!=NULL);
	REQUIRE(result->getSize()==6);
	actual=result->getRowIdsFor(2);
	CHECK(actual[0]==5);
	CHECK(actual[1]==6);
	CHECK(actual[2]==7);
	CHECK(actual[3]==8);
	CHECK(actual[4]==9);
	CHECK(actual[5]==10);		

	R_destroy1();
}

////////////////////////////////////////////
///                 JOIN                 ///
////////////////////////////////////////////

TEST_CASE("Relation::performJoinWithOriginal() - trivial case", "[JOIN]"){
    R_init1();
    R[0]->setId(0);
    Relation Empty(0, 3);
    Empty.setId(1);
    IntermediateRelation *result = R[0]->performJoinWithOriginal(Empty, 0, 1, 1, 1);
    REQUIRE( result != NULL );
    CHECK( result->getSize() == 0 );
    delete result;
    result = Empty.performJoinWithOriginal(*R[0], 1, 1, 0, 1);
    REQUIRE( result != NULL );
    CHECK( result->getSize() == 0 );
    delete result;
    Relation Empty2(0, 5);
    Empty2.setId(2);
    result = Empty.performJoinWithOriginal(Empty2, 1, 2, 2, 1);
    REQUIRE( result != NULL );
    CHECK( result->getSize() == 0 );
    delete result;
    R_destroy1();
}

TEST_CASE("Relation::performJoinWithOriginal()", "[JOIN]"){
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

TEST_CASE("IntermediateRelation::performJoinWithOriginal() - trivial case", "[JOIN]"){
    R_init1();
    R[0]->setId(0);
    R[1]->setId(1);
    Relation Empty(0, 3);
    Empty.setId(2);
    unsigned int rowids[] = {1, 3, 4, 6};
    IntermediateRelation I(1, rowids, 4, R[1]);
    IntermediateRelation *result = I.performJoinWithOriginal(Empty, 1, 1, 2, 1);
    REQUIRE( result != NULL );
    CHECK( result->getSize() == 0 );
    IntermediateRelation I_Empty(3, NULL, 0, R[1]);
    result = I_Empty.performJoinWithOriginal(Empty, 3, 1, 2, 1);
    REQUIRE( result != NULL );
    CHECK( result->getSize() == 0 );
    R_destroy1();
}

TEST_CASE("IntermediateRelation::performJoinWithOriginal()", "[JOIN]"){
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
        res.insert(make_pair(actual0[i], actual1[i]));
    }
    CHECK( res.find(make_pair(1, 1)) != res.end() );
    CHECK( res.find(make_pair(2, 1)) != res.end() );
    CHECK( res.find(make_pair(3, 1)) != res.end() );
    CHECK( res.find(make_pair(4, 4)) != res.end() );
    CHECK( res.find(make_pair(5, 4)) != res.end() );
    CHECK( res.find(make_pair(7, 6)) != res.end() );
    CHECK( res.find(make_pair(8, 6)) != res.end() );
    R_destroy1();
}

TEST_CASE("IntermediateRelation::performJoinWithIntermediate() - trivial case", "[JOIN]"){
    R_init1();
    unsigned int rowids[] = {1, 3, 4, 6};
    IntermediateRelation I1(0, rowids, 4, R[0]);
    IntermediateRelation I2(1, (unsigned int *) NULL, 0, R[1]);
    IntermediateRelation *result = I1.performJoinWithIntermediate(I2, 1, 1, 0, 1);
    REQUIRE( result != NULL );
    CHECK( result->getSize() == 0 );
    result = I2.performJoinWithIntermediate(I1, 0, 1, 1, 1);
    REQUIRE( result != NULL );
    CHECK( result->getSize() == 0 );
    R_destroy1();
}

TEST_CASE("IntermediateRelation::performJoinWithIntermediate()", "[JOIN]"){
    R_init1();
    R[0]->setId(0);
    R[1]->setId(1);
    unsigned int rowids1[] = {2, 4, 7, 8};
    IntermediateRelation I1(0, rowids1, 4, R[0]);
    unsigned int rowids2[] = {4, 5, 6};
    IntermediateRelation I2(1, rowids2, 3, R[1]);
    IntermediateRelation *result = I1.performJoinWithIntermediate(I2, 0, 1, 1, 1);
    const unsigned int resultSize = 4;
    REQUIRE( result->getSize() == resultSize );
    const unsigned int *actual0 = result->getRowIdsFor(0);
    const unsigned int *actual1 = result->getRowIdsFor(1);
    set<pair<unsigned int, unsigned int>> res;
    for (int i = 0 ; i < resultSize ; i++){
        res.insert(make_pair(actual0[i], actual1[i]));
    }
    CHECK( res.find(make_pair(4, 4)) != res.end() );
    CHECK( res.find(make_pair(4, 5)) != res.end() );
    CHECK( res.find(make_pair(7, 6)) != res.end() );
    CHECK( res.find(make_pair(8, 6)) != res.end() );
    R_destroy1();
}


////////////////////////////////////////////
///             CROSS PRODUCT            ///
////////////////////////////////////////////

TEST_CASE("Relation::performCrossProductWithOriginal() - trivial case", "[CROSS PRODUCT]"){
    R_init2();
    R[0]->setId(0);
    Relation Empty(0, 3);
    Empty.setId(1);
    IntermediateRelation *result = R[0]->performCrossProductWithOriginal(Empty);
    REQUIRE( result != NULL );
    CHECK( result->getSize() == 0 );
    delete result;
    result = Empty.performCrossProductWithOriginal(*R[0]);
    REQUIRE( result != NULL );
    CHECK( result->getSize() == 0 );
    delete result;
    Relation Empty2(0, 5);
    Empty2.setId(2);
    result = Empty.performCrossProductWithOriginal(Empty2);
    REQUIRE( result != NULL );
    CHECK( result->getSize() == 0 );
    delete result;
    R_destroy2();
}

TEST_CASE("Relation::performCrossProductWithOriginal()", "[CROSS PRODUCT]"){
    R_init2();
    R[0]->setId(0);
    R[1]->setId(1);
    IntermediateRelation *result = R[0]->performCrossProductWithOriginal(*R[1]);
    REQUIRE( result != NULL );
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

TEST_CASE("IntermediateRelation::performCrossProductWithOriginal() - trivial case", "[CROSS PRODUCT]"){
    R_init1();
    R[0]->setId(0);
    R[1]->setId(1);
    Relation Empty(0, 3);
    Empty.setId(2);
    unsigned int rowids[] = {1, 3, 4, 6};
    IntermediateRelation I(1, rowids, 4, R[1]);
    IntermediateRelation *result = I.performCrossProductWithOriginal(Empty);
    REQUIRE( result != NULL );
    CHECK( result->getSize() == 0 );
    IntermediateRelation I_Empty(3, NULL, 0, R[1]);
    result = I_Empty.performCrossProductWithOriginal(Empty);
    REQUIRE( result != NULL );
    CHECK( result->getSize() == 0 );
    R_destroy1();
}

TEST_CASE("IntermediateRelation::performCrossProductWithOriginal()", "[CROSS PRODUCT]"){
    R_init2();
    R[0]->setId(0);
    unsigned int rowids[] = {4, 6, 8, 9};
    IntermediateRelation I(1, rowids, 4, R[1]);
    IntermediateRelation *result = I.performCrossProductWithOriginal(*R[0]);
    REQUIRE( result != NULL );
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

TEST_CASE("IntermediateRelation::performCrossProductWithIntermediate() - trivial case", "[CROSS PRODUCT]"){
    R_init1();
    R[0]->setId(0);
    R[1]->setId(1);
    unsigned int rowids[] = {1, 3, 4, 6};
    IntermediateRelation I1(0, rowids, 4, R[0]);
    IntermediateRelation I2(1, (unsigned int *) NULL, 0, R[1]);
    IntermediateRelation *result = I1.performCrossProductWithIntermediate(I2);
    REQUIRE( result != NULL );
    CHECK( result->getSize() == 0 );
    result = I2.performCrossProductWithIntermediate(I1);
    REQUIRE( result != NULL );
    CHECK( result->getSize() == 0 );
    R_destroy1();
}

TEST_CASE("IntermediateRelation::performCrossProductWithIntermediate()", "[CROSS PRODUCT]") {
    R_init1();
    R[0]->setId(0);
    R[1]->setId(1);
    unsigned int rowids1[] = {1, 3, 4, 6};
    unsigned int rowids2[] = {5, 7};
    IntermediateRelation I1(0, rowids1, 4, R[0]);
    IntermediateRelation I2(1, rowids2, 2, R[1]);
    IntermediateRelation *result = I1.performCrossProductWithIntermediate(I2);
    REQUIRE( result != NULL );
    const unsigned int resultSize = 8;
    REQUIRE( result->getSize() == resultSize );
    const unsigned int *actual0 = result->getRowIdsFor(0);
    const unsigned int *actual1 = result->getRowIdsFor(1);
    set<pair<unsigned int, unsigned int>> res;
    for (int i = 0 ; i < resultSize ; i++){
        res.insert(make_pair(actual0[i], actual1[i]));
    }
    CHECK( res.find(make_pair(1, 5)) != res.end() );
    CHECK( res.find(make_pair(1, 7)) != res.end() );
    CHECK( res.find(make_pair(3, 5)) != res.end() );
    CHECK( res.find(make_pair(3, 7)) != res.end() );
    CHECK( res.find(make_pair(4, 5)) != res.end() );
    CHECK( res.find(make_pair(4, 7)) != res.end() );
    CHECK( res.find(make_pair(6, 5)) != res.end() );
    CHECK( res.find(make_pair(6, 7)) != res.end() );
    R_destroy1();
}
