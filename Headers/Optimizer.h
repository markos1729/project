#ifndef OPTIMIZER_H
#define OPTIMIZER_H

#include <unordered_map>
#include "SQLParser.h"
#include "FieldTypes.h"
#include "ConfigureParameters.h"
#include "Relation.h"


#define BIG_N 50000000


using namespace std;


struct RelationStats {
    const Relation *R;
    const unsigned int ncol;   // number of columns
    unsigned int f;     // number of rows (same for each column)
    intField *l;        // minimum value for each column
    intField *u;        // maximum value for each column
    unsigned int *d;    // number of distinct values for each column
    unsigned int *N;    // bitmap size for each column
    uint64_t **bitmap;  // compact bitmap for each column
    ////////////////////////////////////////////////////////////////////////
    explicit RelationStats(const Relation *_R);
    RelationStats(const RelationStats& relStats, bool keep_bitmap = false);
    ~RelationStats();
    bool calculateStats();
#ifdef DDEBUG
    void printStats(int relId) {
        printf("R%d stats: f=%d\n", relId, f);
        if (f == 0) {
            printf("  column stats N/A\n");
            return;
        }
        for (int c = 0; c < ncol; c++) {
            printf("  col%d: { l:%lu | u:%lu | d:%d }\n", c, l[c], u[c], d[c]);
        }
    }
#endif
};


class Optimizer {
    unsigned int nrel;         // number of relations
    RelationStats **relStats;
    const SQLParser &parser;   // parser for this query

    class JoinTree {
    public:
        unsigned int treeF;
        unordered_map<unsigned int, const RelationStats*> relationsStats;

        const RelationStats *allocated1, *allocated2;   // only used for deletion

        int *predsOrder;
        int predsOrderIndex;
        bool *predsJoined;

        JoinTree(unsigned int relId, RelationStats *relStats, unsigned int npredicates);
        JoinTree(JoinTree *currBestTree, unsigned int relId, const RelationStats *relStats, const SQLParser &parser);
        ~JoinTree();
        int bestJoinWithRel(const SQLParser &parser, unsigned int relbId, const RelationStats *relbStats);
    };

public:
    explicit Optimizer(const SQLParser &_parser);
    ~Optimizer();
    void initializeRelation(unsigned int rid, const RelationStats *stats);
    void estimate_filters();     // changes relStats accordingly
    void estimate_eqColumns();   // changes relStats accordingly
    int *best_plan();            // creates new relStats in JoinTrees
#ifdef DDEBUG
    void printAllRelStats() {
        for (int i = 0; i < nrel; i++) {
            relStats[i]->printStats(i);
            cout << endl;
        }
    }
#endif
private:
    bool connected(int RId, string SIdStr);
};

#endif
