#ifndef OPTIMIZER_H
#define OPTIMIZER_H

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
    explicit RelationStats(const Relation *_R);;
    ~RelationStats();

    //TODO: need this?
    explicit RelationStats(unsigned int num_of_columns, unsigned int _f) : R(NULL), ncol(num_of_columns), f(_f) {
        l = new intField[ncol]();
        u = new intField[ncol]();
        d = new unsigned int[ncol]();
        N = new unsigned int[ncol]();
        bitmap = new uint64_t*[ncol]();   // init pointers to NULL (!)
    };

    //TODO: need this?
    RelationStats(const RelationStats& relStats) : R(relStats.R), ncol(relStats.ncol), f(relStats.f) {
        l = new intField[ncol];
        u = new intField[ncol];
        d = new unsigned int[ncol];
        for (int i = 0; i < ncol; i++) {
            l[i] = relStats.l[i];
            u[i] = relStats.u[i];
            d[i] = relStats.d[i];
        }
        // N, bitmap should not be used in objects created as such
        N = NULL;
        bitmap = NULL;
    }

    //TODO: need this?
    RelationStats& operator= (const RelationStats& relStats) {
        if(&relStats == this) return *this;
        R = relStats.R;
        f = relStats.f;
        delete[] l;
        l = new intField[R.getNumOfColumns()];
        delete[] u;
        u = new intField[R.getNumOfColumns()];
        delete[] d;
        d = new unsigned int[ncol];
        for (int i = 0; i < ncol; i++) {
            l[i] = relStats.l[i];
            u[i] = relStats.u[i];
            d[i] = relStats.d[i];
        }
        // N, bitmap should not be used in objects created as such
        N = NULL;
        bitmap = NULL;
        return *this;
    }
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
        unordered_map<unsigned int, RelationStats*> relationsStats;
        RelationStats* relStatsCreatedPtr;      // used for its deletion
        int *predsOrder;
        int predsOrderIndex;
        bool *predsJoined;

        JoinTree(unsigned int relId, RelationStats *relStats, unsigned int npredicates);
        JoinTree(JoinTree *currBestTree, unsigned int relId, RelationStats *relStats, const SQLParser &parser);
        ~JoinTree();
        int bestJoinWithRel(const SQLParser &parser, unsigned int relbId, RelationStats *relbStats);
    };

public:
    explicit Optimizer(const SQLParser &_parser);
    ~Optimizer();
    void initializeRelation(unsigned int rid, RelationStats *stats);
    void estimate_filters();
    void estimate_eqColumns();
    int *best_plan();
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
