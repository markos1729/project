#ifndef OPTIMIZER_H
#define OPTIMIZER_H

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
        JoinTree(int _nrel, int _ncol, int relId, intField *relL, intField *relU, unsigned int relF, unsigned int *relD, unsigned int npredicates);
        JoinTree(JoinTree *currBestTree, int relId, intField *relL, intField *relU, unsigned int relF, unsigned int *relD, SQLParser parser);
        ~JoinTree();
        int calcJoinStats(SQLParser parser, int relId, unsigned int relF, unsigned int *relD, unsigned int *newTreeF, unsigned int **newTreeD);
    };

public:
    Optimizer(SQLParser _parser);
    ~Optimizer();
    void initialize(unsigned int rid, unsigned int rows, unsigned int cols, intField **columns);
    int *best_plan(bool **predsJoined);
};

#endif
