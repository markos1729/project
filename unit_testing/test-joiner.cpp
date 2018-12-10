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

////////////////////////////////////////////
///                JOINER                ///
////////////////////////////////////////////
#include <unistd.h>
#include <wait.h>
#include <cstring>

#define CHECK_PERROR(call, msg, actions) { if ( (call) < 0 ) { perror(msg); actions } }

#define MAX_RESULTS_SIZE 65536
#define ESTIMATED_WAIT_TIME 8
#define JOINER_EXE_PATH "./joiner"

// (!) IMPORTANT: Make sure PRINT_FEEDBACK_MESSAGES is NOT defined in joiner-main.cpp and PRINT_SUM is!

bool setUpJoiner(int &toJoiner_fd, int &fromJoiner_fd, pid_t &pid){
    int toJoinerPipe[2];
    int fromJoinerPipe[2];
    CHECK_PERROR( pipe(toJoinerPipe), "pipe creation failed", return false; )
    CHECK_PERROR( pipe(fromJoinerPipe), "pipe creation failed", return false; )
    CHECK_PERROR( (pid = fork()), "fork failed", return false; )
    if (pid == 0){   // child will become joiner
        // redirect stdin and stdout
        dup2(toJoinerPipe[1], STDIN_FILENO);
        dup2(fromJoinerPipe[0], STDOUT_FILENO);
        close(toJoinerPipe[0]);
        close(toJoinerPipe[1]);
        close(fromJoinerPipe[0]);
        close(fromJoinerPipe[1]);
        // and call exec
        execl(JOINER_EXE_PATH, "joiner", NULL);
        // if failed:
        perror("exec failed: ");
        exit(-404);
    } else if (pid > 0){
        toJoiner_fd = dup(toJoinerPipe[0]);
        fromJoiner_fd = dup(fromJoinerPipe[1]);
        close(toJoinerPipe[0]);
        close(toJoinerPipe[1]);
        close(fromJoinerPipe[0]);
        close(fromJoinerPipe[1]);
    }
    return true;
}



TEST_CASE("Joiner for small input", "[JOINER]"){
    int toJoiner_fd, fromJoiner_fd;
    pid_t pid;
    if ( !setUpJoiner(toJoiner_fd, fromJoiner_fd, pid) || pid < 0 ){
        cerr << "Could not set up a test for joiner" << endl;
        close(toJoiner_fd);
        close(fromJoiner_fd);
        return;
    }
    sleep(1);
    char load_input[] = "Files/r0\nFiles/r1\nFiles/r2\nFiles/r3\nFiles/r4\nFiles/r5\nFiles/r6\nFiles/r7\nFiles/r8\nFiles/r9\nFiles/r10\nFiles/r11\nFiles/r12\nFiles/r13\nDone\n";
    write(toJoiner_fd, load_input, strlen(load_input));
    char workload[] = "3 0 1|0.2=1.0&0.1=2.0&0.2>3499|1.2 0.1\n5 0|0.2=1.0&0.3=9881|1.1 0.2 1.0\n9 0 2|0.1=1.0&1.0=2.2&0.0>12472|1.0 0.3 0.4\n9 0|0.1=1.0&0.1>1150|0.3 1.0\n6 1 12|0.1=1.0&1.0=2.2&0.0<62236|1.0\n11 0 5|0.2=1.0&1.0=2.2&0.1=5784|2.3 0.1 0.1\n4 1 2 11|0.1=1.0&1.0=2.1&1.0=3.1&0.1>2493|3.2 2.2 2.1\n10 0 13 1|0.2=1.0&1.0=2.2&0.1=3.0&0.1=209|0.2 2.5 2.2\n6 1 11 5|0.1=1.0&1.0=2.1&1.0=3.1&0.0>44809|2.0\n3 1|0.1=1.0&0.2<3071|0.2 0.2\nF\n3 1 12|0.1=1.0&1.0=2.1&0.0>26374|2.0 0.1 2.1\n7 0|0.1=1.0&0.4<9936|0.4 0.0 1.0\n2 1 9|0.1=1.0&1.0=2.2&0.1=10731|1.2 2.3\n5 1|0.1=1.0&0.2=4531|1.2\n3 0 13 13|0.2=1.0&1.0=2.1&2.1=3.2&0.2<74|1.2 2.5 3.5\n9 1|0.2=1.0&0.1=1574|0.1 0.3 0.0\n0 5|0.0=1.2&1.3=9855|1.1 0.1\n11 0 2|0.2=1.0&1.0=2.2&0.1<5283|0.0 0.2 2.3\n8 0 7|0.2=1.0&1.0=2.1&0.3>10502|1.1 1.2 2.5\n9 1 11|0.2=1.0&1.0=2.1&1.0=0.2&0.3>3991|1.0\n4 1|0.1=1.0&0.1<5730|1.1 0.1 0.1\n3 1 5 7|0.1=1.0&1.0=2.1&1.0=3.2&0.2=4273|2.2 3.2\nF\n9 1 12|0.2=1.0&1.0=2.1&2.2=1.0&0.2<2685|2.0\n1 12 2|0.0=1.2&0.0=2.1&1.1=0.0&1.0>25064|0.2 1.3\n2 0|0.2=1.0&0.2<787|0.0\n1 6|0.0=1.1&1.1>10707|1.0 1.1 0.2\n13 0 3|0.1=1.0&1.0=2.2&0.4=10571|2.3 0.0\n12 1 6 12|0.2=1.0&1.0=2.1&0.1=3.2&3.0<33199|2.1 0.1 0.2\n11 0 10 8|0.2=1.0&1.0=2.2&1.0=3.2&0.0<9872|3.3 2.2\n11 0 2|0.2=1.0&1.0=2.2&0.1<4217|1.0\n10 0|0.2=1.0&0.2>1791|1.0 1.2 0.2\n7 1 3|0.2=1.0&1.0=2.1&0.3<8722|1.0\n4 1 9|0.1=1.0&1.0=2.2&0.1>345|0.0 1.2\n11 1 12 10|0.1=1.0&1.0=2.1&1.0=3.1&0.2=598|3.2\n7 0 9|0.1=1.0&1.0=0.1&1.0=2.1&0.1>3791|1.2 1.2\nF\n8 0 11|0.2=1.0&1.0=2.2&0.3=9477|0.2\n0 13 7 10|0.0=1.2&0.0=2.1&0.0=3.2&1.2>295|3.2 0.0\n7 1 3|0.2=1.0&1.0=2.1&1.0=0.2&0.2>6082|2.3 2.1\n0 7 10 5|0.0=1.1&0.0=2.2&0.0=3.2&1.3=8728|2.0 3.1\n1 4 9 8|0.0=1.1&0.0=2.2&0.0=3.1&1.1>2936|1.0 1.0 3.0\n4 1|0.1=1.0&0.1<9795|1.2 0.1\n11 1|0.1=1.0&0.1<1688|0.1\n5 0|0.2=1.0&0.0<1171|1.0 0.3\n4 1 6|0.1=1.0&1.0=2.1&0.0<13500|2.1 0.1 0.0\n13 13|0.1=1.2&1.6=8220|1.5\nF\n11 0 8|0.2=1.0&1.0=2.2&0.2>4041|1.0 1.1 1.0\n8 0 10|0.2=1.0&1.0=2.2&0.3<9473|0.3 2.0\n5 1 8|0.1=1.0&1.0=2.1&0.1<3560|1.2\n13 0 2|0.2=1.0&1.0=0.1&1.0=2.2&0.1>4477|2.0 2.3 1.2\n8 0 13 13|0.2=1.0&1.0=2.2&2.1=3.2&0.1>7860|3.3 2.1 3.6\nF\n";
    write(toJoiner_fd, workload, strlen(workload));
    sleep(ESTIMATED_WAIT_TIME);
    // results should be ready by now
    char *results = new char[MAX_RESULTS_SIZE];
    ssize_t nread = read(fromJoiner_fd, results, MAX_RESULTS_SIZE - 1);
    if (nread < 0){
        cerr << "Could not get an answer from joiner" << endl;
        close(toJoiner_fd);
        close(fromJoiner_fd);
        return;
    }
    results[nread] = '\0';
    cout << "nread = " << nread << endl;
    char expectedresults[] = "26468015 32533054\n5446 1009 1009\n31831879 99876596 96864400\n27314139 10766320\n901496306\nNULL NULL NULL\n281654532 282357938 841559324\n187822 1036243 187822\n1771710026\n22766314 22766314\n4634779503 627329747 627329747\n41316024 267394881 9912745\nNULL NULL\n9283\n760151704 2430170493 2398594221\nNULL NULL NULL\nNULL NULL\n3537544236 319415951 334176745\n1645366150 1016283918 788397597\n106938127\n10014140 6526034 6526034\nNULL NULL\n81073\n265717 299090\n179170057\n65339549 18594932 12524601\n744497 7983214\n3377819501 3395449560 3377819501\n5032407477 1146864253\n255982520\n23837499 33633760 23837499\n1391837538\n42163975 42772523\n858534\n109050774 109050774\n444547\n20504095556 20504095556\n633080591 1050051803\n5304879 1634121\n259497861 259497861 1733901713\n28536640 19373870\n2135404\n943464 3320201\n178397045 178397045 213214740\n852794\n203444887 336237721 203444887\n230229977 440364776\n93772438\n840902 45292 63810\n103260116758 17416413522 59644305653\n";
    CHECK( strcmp(results, expectedresults) == 0 );
    cout << results << endl;
    delete[] results;
    int status;
    waitpid(pid, &status, WNOHANG);
}
