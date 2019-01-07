#ifndef OPTIMIZER_H
#define OPTIMIZER_H

#include "SQLParser.h"
#include "FieldTypes.h"

using namespace std;

class RelationStats {
public:
    unsigned int ncol;
    unsigned int f;     // number of rows (same for each column)
    intField *l;        // minimum value for each column
    intField *u;        // maximum value for each column
    unsigned int *d;    // number of distinct values for each column

    explicit RelationStats(unsigned int _ncol) : ncol(_ncol), f(0) {
        l = new intField[ncol];
        u = new intField[ncol];
        d = new unsigned int[ncol];
    };
    RelationStats(const RelationStats& relStats) : ncol(relStats.ncol), f(relStats.f) {
        l = new intField[ncol];
        u = new intField[ncol];
        d = new unsigned int[ncol];
        for (int i = 0; i < ncol; i++) {
            l[i] = relStats.l[i];
            u[i] = relStats.u[i];
            d[i] = relStats.d[i];
        }
    }
    RelationStats& operator= (const RelationStats& relStats) {
        ncol = relStats.ncol;
        f = relStats.f;
        delete[] l;
        l = new intField[ncol];
        delete[] u;
        u = new intField[ncol];
        delete[] d;
        d = new unsigned int[ncol];
        for (int i = 0; i < ncol; i++) {
            l[i] = relStats.l[i];
            u[i] = relStats.u[i];
            d[i] = relStats.d[i];
        }
        return *this;
    };
    ~RelationStats() {
        delete[] l;
        delete[] u;
        delete[] d;
    }
};

class Optimizer {
    unsigned int nrel;  // number of relations
    RelationStats **relStats;

    SQLParser parser;   // parser for this query
    unsigned int **N;   // bitmap size for each column
    uint64_t ***bitmap; // compact bitmap for each column

    void filter();
    bool connected(int RId, string SIdStr);

    class JoinTree {
    public:
        unsigned int treeF;
        unordered_map<unsigned int, RelationStats*> relationsStats;
        int *predsOrder;
        int nextPredOrder;

        JoinTree(unsigned int relId, RelationStats *relStats, unsigned int npredicates);
        JoinTree(JoinTree *currBestTree, unsigned int relId, RelationStats *relStats, const SQLParser &parser);
        ~JoinTree();
        int bestJoinWithRel(const SQLParser &parser, unsigned int relbId, RelationStats *relbStats);
    };

public:
    explicit Optimizer(const SQLParser &_parser);
    ~Optimizer();
    void initializeRelation(unsigned int rid, unsigned int rows, unsigned int cols, intField **columns);
    int *best_plan();
};

#endif
