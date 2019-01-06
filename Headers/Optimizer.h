#ifndef OPTIMIZER_H
#define OPTIMIZER_H

#include <vector>
#include "SQLParser.h"
#include "FieldTypes.h"
using namespace std;

class Optimizer {
private:
    unsigned int nrel;  // number of relations
    unsigned int *ncol; // number of columns for each relation

    intField **L;       // minimum value for each column
    intField **U;       // maximum value for each column
    unsigned int *F;    // number of rows (same for each column)
    unsigned int **D;   // number of distinct values for each column

    SQLParser parser;   // parser for this query
    unsigned int **N;   // bitmap size for each column
    uint64_t ***bitmap; // compact bitmap for each column

    void filter();
    bool connected(int RId, string SIdStr);

    class JoinTree {
    public:
        int treeNrel;
        int *treeNcol;
        int *rowJoinOrder;
        int nextRelOrder;
        intField *treeL;
        intField *treeU;
        unsigned int treeF;
        unsigned int *treeD;
        bool *predicatesJoined;
        JoinTree(int _nrel, int _ncol, int relId, intField *relL, intField *relU, unsigned int relF, unsigned int *relD, unsigned int npredicates)
                : treeNrel(_nrel) {
            treeNcol = new int[_nrel];
            treeL = new intField[_nrel];
            treeU = new intField[_nrel];
            treeD = new unsigned int[_nrel];
            // TODO: Initialize the above??
            rowJoinOrder = new int[_nrel]();
            rowJoinOrder[relId] = 1;
            nextRelOrder = 2;
            predicatesJoined = new bool[npredicates]();
        };
        JoinTree(JoinTree *currBestTree, int relId, intField *relL, intField *relU, unsigned int relF, unsigned int *relD, SQLParser parser);
        ~JoinTree() {
            delete[] rowJoinOrder;
            delete[] predicatesJoined;
            // TODO: delete whatever else ends up being allocated by constructor
        };
        int calcJoinStats(SQLParser parser, int relId, unsigned int relF, unsigned int *relD, unsigned int *newTreeF, unsigned int **newTreeD);
    };

public:
    Optimizer(SQLParser _parser) : nrel(_parser.nrelations), parser(_parser) {
        ncol = new unsigned int[nrel];
        L = new intField*[nrel];
        U = new intField*[nrel];
        F = new unsigned int[nrel];
        D = new unsigned int*[nrel];
        N = new unsigned int*[nrel];
        bitmap = new uint64_t**[nrel];
    }

    ~Optimizer() {
        for (unsigned int r = 0; r < nrel; r++) {
            delete[] L[r];
            delete[] U[r];
            delete[] F;
            delete[] D[r];
            delete[] N[r];
            delete L;
            delete U;
            delete D;
            delete N;
            for (unsigned int c = 0; c < ncol[r]; c++) delete[] bitmap[r][c];
            delete bitmap[r];
            delete bitmap;
        }
        delete[] ncol;
    }

    void initialize(unsigned int rid, unsigned int rows, unsigned int cols, intField **columns);
    int *best_plan(bool **predsJoined);
};


#endif
