#include <algorithm>
#include <cstdio>
#include <utility>
#include <set>

#include "catch.hpp"
#include "../Headers/Relation.h"
#include "../Headers/FieldTypes.h"
#include "../Headers/SQLParser.h"
#include "../Headers/Optimizer.h"

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

void (*R_destroy3)() = R_destroy1;

void R_init3() {
    if (R != NULL){
        R_destroy3();
    }
    R = new Relation*[3];
    Rlen = 3;
    {   ////////////////////////////
        R[0] = new Relation(10, 2);
        intField col1[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        intField col2[] = {5, 5, 5, 8, 5, 12, 16, 5, 32, 5};
        R[0]->addColumn(0, col1);
        R[0]->addColumn(1, col2);
    }
    {   ////////////////////////////
        R[1] = new Relation(6, 2);
        intField col1[] = {5, 3, 1, 1, 9, 4};
        intField col2[] = {5, 7, 42, 8, 3, 3};
        R[1]->addColumn(0, col1);
        R[1]->addColumn(1, col2);
    }
    {   ////////////////////////////
        R[2] = new Relation(5, 3);
        intField col1[] = {3, 3, 3, 3, 3, 3};
        intField col2[] = {6, 6, 6, 6, 6, 6};
        intField col3[] = {2, 3, 4, 5, 6, 7};
        R[2]->addColumn(0, col1);
        R[2]->addColumn(1, col2);
        R[2]->addColumn(2, col3);
    }
}

void (*R_destroy4)() = R_destroy1;

void R_init4() {
    if (R != NULL){
        R_destroy4();
    }
    R = new Relation*[3];
    Rlen = 3;
    {   ////////////////////////////
        R[0] = new Relation(10, 2);
        intField col1[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        intField col2[] = {1, 1, 3, 3, 5, 5, 7, 7, 9, 9};
        R[0]->addColumn(0, col1);
        R[0]->addColumn(1, col2);
    }
    {   ////////////////////////////
        R[1] = new Relation(6, 2);
        intField col1[] = {1, 1, 2, 2, 4, 4};
        intField col2[] = {7, 7, 7, 7, 7, 7};
        R[1]->addColumn(0, col1);
        R[1]->addColumn(1, col2);
    }
    {   ////////////////////////////
        R[2] = new Relation(5, 3);
        intField col1[] = {3, 3, 3, 3, 3, 3};
        intField col2[] = {6, 6, 6, 6, 6, 6};
        intField col3[] = {2, 3, 4, 5, 6, 7};
        R[2]->addColumn(0, col1);
        R[2]->addColumn(1, col2);
        R[2]->addColumn(2, col3);
    }
}

/*
TEST_CASE("Optimizer::estimate_filters()", "[FILTER]") {
    R_init3();
    RelationStats **Rstats = new RelationStats *[Rlen];
    for (int i = 0 ; i < Rlen ; i++) {
        Rstats[i] = new RelationStats(R[i]);
        Rstats[i]->calculateStats();
    }

    SQLParser *p = new SQLParser("0 1 2|0.1=5&1.0>4&2.2<3|0.0");
    printf("---------------------------------\n");
    Optimizer *optimizer = new Optimizer(*p);
    for (unsigned int i = 0; i < p->nrelations; i++) {
        optimizer->initializeRelation(i, Rstats[p->relations[i]]);
    }
    optimizer->printAllRelStats();
    printf("---------------------------------\n");
    optimizer->estimate_filters();
    optimizer->printAllRelStats();
    printf("=================================\n");
    delete optimizer;
    delete p;
    R_destroy3();
}

TEST_CASE("Optimizer::estimate_eqColumns()", "[FILTER]") {
    R_init3();
    RelationStats **Rstats = new RelationStats *[Rlen];
    for (int i = 0 ; i < Rlen ; i++) {
        Rstats[i] = new RelationStats(R[i]);
        Rstats[i]->calculateStats();
    }

    SQLParser *p = new SQLParser("0 1 2|0.0=0.1&2.0=2.1|0.0");
    printf("---------------------------------\n");
    Optimizer *optimizer = new Optimizer(*p);
    for (unsigned int i = 0; i < p->nrelations; i++) {
        optimizer->initializeRelation(i, Rstats[p->relations[i]]);
    }
    optimizer->printAllRelStats();
    printf("---------------------------------\n");
    optimizer->estimate_eqColumns();
    optimizer->printAllRelStats();
    printf("=================================\n");
    delete optimizer;
    delete p;
    R_destroy3();
}
*/

TEST_CASE("Optimizer::best_plan() - simple", "[BEST_PLAN]") {
    R_init4();
    // "0.0=1.1" produces 6 results, while "0.1=2.0" 12 results
    RelationStats **Rstats = new RelationStats *[Rlen];
    for (int i = 0 ; i < Rlen ; i++) {
        Rstats[i] = new RelationStats(R[i]);
        Rstats[i]->calculateStats();
    }

    SQLParser *p = new SQLParser("0 1 2|0.0=1.1&0.1=2.0|0.0");
    Optimizer *optimizer = new Optimizer(*p);
    for (unsigned int i = 0; i < p->nrelations; i++) {
        optimizer->initializeRelation(i, Rstats[p->relations[i]]);
    }
    optimizer->estimate_filters();
    optimizer->estimate_eqColumns();
    int *bestJoinOrder = optimizer->best_plan();
    CHECK( bestJoinOrder[0] == 0 );
    delete optimizer;
    delete p;
    delete[] bestJoinOrder;

    p = new SQLParser("0 1 2|0.1=2.0&0.0=1.1|0.0");
    optimizer = new Optimizer(*p);
    for (unsigned int i = 0; i < p->nrelations; i++) {
        optimizer->initializeRelation(i, Rstats[p->relations[i]]);
    }
    optimizer->estimate_filters();
    optimizer->estimate_eqColumns();
    bestJoinOrder = optimizer->best_plan();
    CHECK( bestJoinOrder[0] == 1 );
    delete optimizer;
    delete p;
    delete[] bestJoinOrder;
    R_destroy4();
}

TEST_CASE("Optimizer::best_plan() - column prioritizing", "[BEST_PLAN]") {
    R_init4();
    // "0.1=1.1" will be cacluclated to have f=0, unlike "0.1=1.0"
    RelationStats **Rstats = new RelationStats *[Rlen];
    for (int i = 0 ; i < Rlen ; i++) {
        Rstats[i] = new RelationStats(R[i]);
        Rstats[i]->calculateStats();
    }

    SQLParser *p = new SQLParser("1 2|0.0=1.0&0.0=1.1|0.0");
    Optimizer *optimizer = new Optimizer(*p);
    for (unsigned int i = 0; i < p->nrelations; i++) {
        optimizer->initializeRelation(i, Rstats[p->relations[i]]);
    }
    optimizer->estimate_filters();
    optimizer->estimate_eqColumns();
    int *bestJoinOrder = optimizer->best_plan();
    CHECK( bestJoinOrder[0] == 1 );
    delete optimizer;
    delete p;
    delete[] bestJoinOrder;

    // it will still be preferred even if there's an available join between "0" and "1" at another column
    p = new SQLParser("1 2|0.0=1.1&0.0=1.0|0.0");
    optimizer = new Optimizer(*p);
    for (unsigned int i = 0; i < p->nrelations; i++) {
        optimizer->initializeRelation(i, Rstats[p->relations[i]]);
    }
    optimizer->estimate_filters();
    optimizer->estimate_eqColumns();
    bestJoinOrder = optimizer->best_plan();
    CHECK( bestJoinOrder[0] == 0 );
    delete optimizer;
    delete p;
    delete[] bestJoinOrder;
    R_destroy4();
}
