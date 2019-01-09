#include <cmath>
#include <climits>
#include <unordered_map>
#include <algorithm>
#include "../Headers/macros.h"
#include "../Headers/Optimizer.h"

#define BIG_N 50000000

Optimizer::JoinTree::JoinTree(unsigned int relId, RelationStats *relStats, unsigned int npredicates) : treeF(relStats->f), predsOrderIndex(0) {
    relationsStats[relId] = relStats;
	predsOrder = new int[npredicates]();
	predsJoined = new bool[npredicates]();
}

Optimizer::JoinTree::JoinTree(JoinTree *currBestTree, unsigned int relId, RelationStats *relStats, const SQLParser &parser)
		: treeF(currBestTree->treeF), predsOrderIndex(currBestTree->predsOrderIndex) {
	predsOrder = new int[parser.npredicates];
	predsJoined = new bool[parser.npredicates];
	for (int i = 0; i < parser.npredicates; i++) {
		predsOrder[i] = currBestTree->predsOrder[i];
		predsJoined[i] = currBestTree->predsJoined[i];
	}
	relationsStats = currBestTree->relationsStats;	// TODO: what kind of copy do we want here?
	int predJoined = bestJoinWithRel(parser, relId, relStats);
	CHECK( (predJoined > -1), "Error: tried to create Join Tree on non-connected relations", );
    predsOrder[predsOrderIndex] = predJoined;
    predsOrderIndex++;
    predsJoined[predJoined] = true;
}

Optimizer::JoinTree::~JoinTree() {
	auto it = relationsStats.begin();
	while (it != relationsStats.end()) {
		delete it->second;
		it++;
	}
	delete[] predsOrder;
	delete[] predsJoined;
}

/* Performs join between the JoinTree and the given relation using the best predicate (at the best column) according to the calculated stats.
 * It then updates JoinTree's stats and adds the new relation's stats to relationsStats */
int Optimizer::JoinTree::bestJoinWithRel(const SQLParser &parser, unsigned int relbId, RelationStats *relbStats) {
    int predJoined = -1;
    int bestF = -1;
    unsigned int currF;
    unsigned int relaId, cola, colb, joinedRelaId;
    RelationStats *relaStats, *joinedRelaStats;
	RelationStats *joinedRelbStats = relbStats;
    for (int i = 0; i < parser.npredicates; i++) {
        if (relationsStats.find(parser.predicates[i].rela_id) != relationsStats.end() && relbId == parser.predicates[i].relb_id) {      // rela is in tree and relbId is relb
        	relaId = parser.predicates[i].rela_id;
            cola = parser.predicates[i].cola_id;
            colb = parser.predicates[i].colb_id;
		} else if (relationsStats.find(parser.predicates[i].relb_id) != relationsStats.end() && relbId == parser.predicates[i].rela_id) {    // reverse rela and relb so that the follwing code remains the same
			relaId = parser.predicates[i].relb_id;
			cola = parser.predicates[i].colb_id;
			colb = parser.predicates[i].cola_id;
		} else continue;
		currF = float(treeF * relbStats->f) / (relbStats->u[colb] - relbStats->l[colb] + 1);
		if (bestF == -1 || currF < bestF) {         // join at this column is the best join (yet)
            cout << "currF: " << currF << endl;     // DEBUG
			relaStats = relationsStats[relaId];
			joinedRelaStats = relaStats;
			joinedRelaId = relaId;
			// update stats for joined columns
			bestF = joinedRelaStats->f = joinedRelbStats->f = currF;
			joinedRelaStats->d[cola] = float(relaStats->d[cola] * relbStats->d[colb]) / (relbStats->u[colb] - relbStats->l[colb] + 1);
			joinedRelbStats->d[colb] = float(relaStats->d[cola] * relbStats->d[colb]) / (relbStats->u[colb] - relbStats->l[colb] + 1);
			// update stats for the other columns:
			for (int c = 0; c < joinedRelaStats->ncol; c++) {
				if (c == cola) continue;
				joinedRelaStats->d[c] = relaStats->d[c] * (1 - pow((1 - joinedRelaStats->d[cola] / float(relaStats->d[cola])), float(joinedRelaStats->f) / relaStats->d[c]) );
				if (joinedRelaStats->d[c] == 0 && currF > 0) joinedRelaStats->d[c] = 1;
			}
			for (int c = 0; c < joinedRelbStats->ncol; c++) {
				if (c == colb) continue;
				joinedRelbStats->d[c] = relbStats->d[c] * (1 - pow((1 - joinedRelbStats->d[colb] / float(relbStats->d[colb])), float(joinedRelbStats->f) / relbStats->d[c]) );
                if (joinedRelbStats->d[c] == 0 && currF > 0) joinedRelbStats->d[c] = 1;
			}
			predJoined = i;
		}
    }
	CHECK( (predJoined > -1), "Error: bestJoinWithRel() failed to join at all", return -1;);
    treeF = bestF;
	relationsStats[joinedRelaId] = joinedRelaStats;
	relationsStats[relbId] = joinedRelbStats;
    return predJoined;
}

Optimizer::Optimizer(const SQLParser &_parser) : nrel(_parser.nrelations), parser(_parser) {
    relStats = new RelationStats*[nrel];
	N = new unsigned int*[nrel];
	bitmap = new uint64_t**[nrel];
}

Optimizer::~Optimizer() {
	for (unsigned int r = 0; r < nrel; r++) {
		for (unsigned int c = 0; c < relStats[r]->ncol; c++) delete[] bitmap[r][c];
        delete relStats[r];
        delete[] N[r];
		delete[] bitmap[r];
	}
	delete[] relStats;
	delete[] N;
	delete[] bitmap;
}

void Optimizer::initializeRelation(unsigned int rid, unsigned int rows, unsigned int cols, intField **columns) {
	relStats[rid] = new RelationStats(cols);
	N[rid] = new unsigned int[cols];
	bitmap[rid] = new uint64_t*[cols];

	relStats[rid]->f = rows;
	for (unsigned int c = 0; c < cols; c++) {
		intField u, l;
		if (rows == 0) u = l = 0;
		else u = l = columns[c][0];		// currMin = currMax = value of first row
		
		for (unsigned int r = 1; r < rows; r++) {
			l = MIN(l, columns[c][r]);
			u = MAX(u, columns[c][r]);
		}

		relStats[rid]->l[c] = l;
		relStats[rid]->u[c] = u;
		relStats[rid]->d[c] = 0;

		N[rid][c] = MIN(relStats[rid]->u[c] - relStats[rid]->l[c] + 1, BIG_N);
		bitmap[rid][c] = new uint64_t[N[rid][c] / 64+1]();

		for (unsigned int r = 0; r < rows; r++) {
			unsigned int cell = (columns[c][r]-relStats[rid]->l[c]) % N[rid][c];
			if ((bitmap[rid][c][cell / 64] & (1LLU << (cell % 64)) ) == 0) {
				relStats[rid]->d[c]++;
				bitmap[rid][c][cell / 64] |= (1LLU << (cell % 64));
			}
		}
	}
}

void Optimizer::filter() {
	for (unsigned int f = 0; f < parser.nfilters; f++) {
		char cmp = parser.filters[f].cmp;
		intField value = parser.filters[f].value;
		unsigned int rel = parser.filters[f].rel_id;
		unsigned int col = parser.filters[f].col_id;

		unsigned int pF = relStats[rel]->f;
		if (cmp == '=') {
			unsigned int cell = (value - relStats[rel]->l[col]) % N[rel][col];
			relStats[rel]->l[col] = value;
			relStats[rel]->u[col] = value;

			if (bitmap[rel][col][cell/64] & (1LLU<<(cell%64))) {
				relStats[rel]->f = ceil(float(relStats[rel]->f) / relStats[rel]->d[col]);
				relStats[rel]->d[col]=1;
			}
			else relStats[rel]->d[col] = relStats[rel]->f = 0;
		}


		/* some of the formulas below contain "uA - lA" at the denominator which cannot be right, right?
		 * changed these to "uA - lA + 1" as is used elsewhere */
		if (cmp == '<') {
			if (value - 1 >= relStats[rel]->u[col]) continue;
			relStats[rel]->d[col] = ceil(relStats[rel]->d[col] * float(value - relStats[rel]->l[col]) / (relStats[rel]->u[col] - relStats[rel]->l[col] + 1));
			relStats[rel]->f = ceil(relStats[rel]->f * float(value - relStats[rel]->l[col]) / (relStats[rel]->u[col] - relStats[rel]->l[col] + 1));
			relStats[rel]->u[col] = value - 1;
		}
			
		if (cmp == '>') {
			if (value + 1 <= relStats[rel]->l[col]) continue;
			relStats[rel]->d[col] = ceil(relStats[rel]->d[col] * float(-value + relStats[rel]->u[col]) / (relStats[rel]->u[col] - relStats[rel]->l[col] + 1));
			relStats[rel]->f = ceil(relStats[rel]->f * float(- value + relStats[rel]->u[col]) / (relStats[rel]->u[col] - relStats[rel]->l[col] + 1));
			relStats[rel]->l[col] = value + 1;
		}
		
		for (unsigned int c = 0; c < relStats[rel]->ncol; c++) if (c != col) {
			relStats[rel]->d[c] =ceil( relStats[rel]->d[c] * (1.0 - pow((1.0 - float(relStats[rel]->f) / pF), float(relStats[rel]->f) / relStats[rel]->d[c])));
		}
	}
		
	for (unsigned int p=0; p<parser.npredicates; p++) {
		unsigned int rela = parser.predicates[p].rela_id;
		unsigned int relb = parser.predicates[p].relb_id;
		if (rela != relb) continue;
		
		unsigned int cola = parser.predicates[p].cola_id;
		unsigned int colb = parser.predicates[p].colb_id;
		
		unsigned int pF = relStats[rela]->f;

		relStats[rela]->l[cola] = relStats[rela]->l[colb] = MAX(relStats[rela]->l[cola], relStats[rela]->l[colb]);
		relStats[rela]->u[cola] = relStats[rela]->u[colb] = MIN(relStats[rela]->u[cola], relStats[rela]->u[colb]);
		relStats[rela]->f = ceil(float(relStats[rela]->f) / (relStats[rela]->u[cola] - relStats[rela]->l[cola] + 1));
		relStats[rela]->d[cola] = relStats[rela]->d[colb] = ceil(relStats[rela]->d[cola] * (1.0 - pow((1.0 - float(relStats[rela]->f) / pF), float(pF) / relStats[rela]->d[cola]) ));
		
		for (unsigned int c = 0; c < relStats[rela]->ncol; c++) if (c != cola && c != colb) {
			relStats[rela]->d[c] = ceil(relStats[rela]->d[c] * (1.0 - pow((1.0 - float(relStats[rela]->f) / pF), float(relStats[rela]->f) / relStats[rela]->d[c]) ));
		}
	}
}

bool Optimizer::connected(int RId, string SIdStr) {
    /* linearly traversing the predicate list to check for connection between columns might not seem like optimal,
     * but it's bound to be so small in size (<10) that using another data structure would not only complicate things
     * but also probably not even end up being more efficient */
    for (int i = 0; i < parser.npredicates; i++) {
        if ( (parser.predicates[i].rela_id == RId && SIdStr[parser.predicates[i].relb_id] == '1') ||
            (parser.predicates[i].relb_id == RId && SIdStr[parser.predicates[i].rela_id] == '1') )
            return true;
    }
    return false;
}

int *Optimizer::best_plan() {
	unordered_map<string, class JoinTree*> BestTree;
	JoinTree *currTree;
	for (unsigned int i = 0; i < nrel; i++) {
		string RIdStr(nrel, '0');
		RIdStr[i] = '1';
		// e.g. RIdStr for rel 2 with nrel=4 would be "0010" (relIds start from 0)
		currTree = new JoinTree(i, relStats[i], parser.npredicates);
		BestTree[RIdStr] = currTree;
	}
	for (unsigned int i = 1; i < nrel; i++) {
		string SIdStr(nrel - i, '0');
		SIdStr.append(i, '1');
		sort(SIdStr.begin(), SIdStr.end());
		cout << "Level " << i << endl;
		do {	// get all treeIdStr permutations with exactly i '1's
		    if (BestTree[SIdStr] == NULL) continue;
			currTree = NULL;
			for (unsigned int j = 0; j < nrel; j++) {
				// if R[j] is included in S or is not to be joined with S, continue
				if ( SIdStr[j] == '1' || !connected(j, SIdStr) ) continue;
				currTree = new JoinTree(BestTree[SIdStr], j, relStats[j], parser);
				string newSIdStr = SIdStr;
				newSIdStr[j] = '1';
				// if there's no BestTree yet for newS or its cost is greater than currTree
				if ( BestTree.find(newSIdStr) == BestTree.end() || BestTree[newSIdStr]->treeF > currTree->treeF) {
//				    delete BestTree[newSIdStr];
					BestTree[newSIdStr] = currTree;
				} else {
//				    delete currTree;
				}
			}
			if (currTree == NULL) {		// No relations could be joined with currTree; CrossProducts are needed; Aborting best_plan()
				cout << "CrossProducts were found; Aborting BestTree!" << endl;
				// cleaning up BestTree:
//				auto it = BestTree.begin();
//				while (it != BestTree.end()) {
//					delete it->second;
//					it++;
//				}
//				return NULL;
			}
		} while ( next_permutation(SIdStr.begin(), SIdStr.end()) );
	}
	int *bestJoinOrder = new int[parser.npredicates];
	string bestTreeIdStr(nrel, '1');
	currTree = BestTree[bestTreeIdStr];
	cout << "BestF: " << currTree->treeF << endl;            // DEBUG
	for (unsigned int i = 0; i < parser.npredicates; i++) {
		bestJoinOrder[i] = currTree->predsOrder[i];
	}
	for (unsigned int i = 0; i < parser.npredicates; i++) {
		if (currTree->predsJoined[i]) continue;
		bestJoinOrder[currTree->predsOrderIndex] = i;
		currTree->predsOrderIndex++;
	}
	// cleaning up BestTree:
//	auto it = BestTree.begin();
//	while (it != BestTree.end()) {
//		delete it->second;
//		it++;
//	}
	return bestJoinOrder;
}
