#include <cmath>
#include <climits>
#include <unordered_map>
#include <algorithm>
#include "../Headers/macros.h"
#include "../Headers/Optimizer.h"

#define BIG_N 50000000

Optimizer::JoinTree::JoinTree(int _nrel, int _ncol, int relId, intField *relL, intField *relU, unsigned int relF, unsigned int *relD, unsigned int npredicates)
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

Optimizer::JoinTree::JoinTree(JoinTree *currBestTree, int relId, intField *relL, intField *relU, unsigned int relF,
		unsigned int *relD, SQLParser parser) : treeNrel(currBestTree->treeNrel) {
    rowJoinOrder = new int[currBestTree->treeNrel];
    rowJoinOrder[relId] = nextRelOrder;
    nextRelOrder++;
    int predJoined = calcJoinStats(parser, relId, relF, relD, &treeF, &treeD);
	CHECK( (predJoined > -1), "Error: tried to create Join Tree on non-connected relations", );
	predicatesJoined[predJoined] = true;
};

Optimizer::JoinTree::~JoinTree() {
	delete[] rowJoinOrder;
	delete[] predicatesJoined;
	// TODO: delete whatever else ends up being allocated by constructor
};

int Optimizer::JoinTree::calcJoinStats(SQLParser parser, int relId, unsigned int relF, unsigned int *relD,
                  unsigned int *newTreeF, unsigned int **newTreeD) {
    int predJoined = -1;
    int bestF = -1;
    unsigned int currF;
    int cola, colb;
    for (int i = 0; i < parser.npredicates; i++) {
        if (rowJoinOrder[parser.predicates[i].rela_id] > 0 && relId == parser.predicates[i].relb_id) {      // rela is in tree or is relId
            cola = parser.predicates[i].cola_id;
            colb = parser.predicates[i].colb_id;
            currF = (treeF * relF) / (treeU[cola] - treeL[cola] + 1);
            if (bestF == -1 || currF < bestF) {         // join at this column is the best join (yet)
                bestF = *newTreeF = currF;
                *newTreeD[cola] = (treeD[cola] * relD[colb]) / (treeU[cola] - treeL[cola] + 1);
                // TODO: update stats for the other columns:
                // d' = treeD[j] * (1 - pow((1 - *newTreeD[cola] / treeD[cola]), treeF / treeD[j]) )
                for (int j; j < ncola; j++) {
                	if (j == cola) continue;

                }
                for (int j; j < ncolb; j++) {
                	if (j == colb) continue;

                }
				predJoined = i;
            }
        } else if (rowJoinOrder[parser.predicates[i].relb_id] > 0 && relId == parser.predicates[i].rela_id) {
			// TODO: the above, but reversed
        }
    }
    return predJoined;
}

Optimizer::Optimizer(SQLParser _parser) : nrel(_parser.nrelations), parser(_parser) {
		ncol = new unsigned int[nrel];
		L = new intField*[nrel];
		U = new intField*[nrel];
		F = new unsigned int[nrel];
		D = new unsigned int*[nrel];
		N = new unsigned int*[nrel];
		bitmap = new uint64_t**[nrel];
}

Optimizer::~Optimizer() {
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

void Optimizer::initialize(unsigned int rid,unsigned int rows,unsigned int cols,intField **columns) {
	ncol[rid] = cols;
	L[rid] = new intField[cols];
	U[rid] = new intField[cols];
	D[rid] = new unsigned int[cols];
	N[rid] = new unsigned int[cols];
	bitmap[rid] = new uint64_t*[cols];
	
	for (unsigned int c = 0; c < cols; c++) {
        intField u = 0;
		intField i = ULLONG_MAX;
		
		for (unsigned int r = 0; r < rows; r++) {
			i = MIN(i, columns[c][r]);
			u = MAX(u, columns[c][r]);
		}
		
		L[rid][c] = i;
		U[rid][c] = u;
		F[rid] = rows;
		D[rid][c]=0;
	
		N[rid][c] = MIN(U[rid][c]-L[rid][c], BIG_N);
		bitmap[rid][c] = new uint64_t[N[rid][c]/64+1]();
		
		for (unsigned int r = 0; r < rows; r++) {
			unsigned int cell = (columns[c][r]-L[rid][c]) % N[rid][c];
			if (bitmap[rid][c][cell/64] & (1<<(cell%64))==0) {
				D[rid][c]++;
				bitmap[rid][c][cell/64]|=1<<(cell%64);
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

		unsigned int pF = F[rel];
		
		if (cmp == '=') {
			L[rel][col] = value;
			U[rel][col] = value;
			unsigned int cell = value % N[rel][col];
			
			if (bitmap[rel][col][cell/64] & (1<<(cell%64))) {
				F[rel] = F[rel] / D[rel][col];
				D[rel][col]=1;
			}
			else D[rel][col] = F[rel] = 0;
		}
		
		if (cmp == '<') {
			if (value-1>=U[rel][col]) continue;
			U[rel][col]=value-1;
			D[rel][col] = D[rel][col]*double(value-1-L[rel][col])/(U[rel][col]-L[rel][col]);
			F[rel] = F[rel]*double(value-1-L[rel][col])/(U[rel][col]-L[rel][col]);
		}
			
		if (cmp == '>') {
			if (value+1 <= L[rel][col]) continue;
			L[rel][col] = value+1;
			D[rel][col] = D[rel][col]*double(-value-1+U[rel][col])/(U[rel][col]-L[rel][col]);
			F[rel] = F[rel]*double(-value-1+U[rel][col])/(U[rel][col]-L[rel][col]);
		}
		
		for (unsigned int c = 0; c < ncol[rel]; c++) if (c != col) {
			D[rel][c] = D[rel][c]*(1-pow((1-float(F[rel])/pF),float(F[rel])/D[rel][c]));
		}
	}
		
	for (unsigned int p=0; p<parser.npredicates; p++) {
		unsigned int rela = parser.predicates[p].rela_id;
		unsigned int relb = parser.predicates[p].relb_id;
		if (rela != relb) continue;
		
		unsigned int cola = parser.predicates[p].cola_id;
		unsigned int colb = parser.predicates[p].colb_id;
		
		unsigned int pF = F[rela];
		
		L[rela][cola] = L[rela][colb] = MAX(L[rela][cola], L[rela][colb]);
		U[rela][cola] = U[rela][colb] = MIN(U[rela][cola], U[rela][colb]);
		F[rela] = F[rela]/(U[rela][cola]-L[rela][cola]+1);
		D[rela][cola] = D[rela][colb] = D[rela][cola]*(1-pow((1-float(F[rela])/pF),float(pF)/D[rela][cola]));
		
		for (unsigned int c = 0; c < ncol[rela]; c++) if (c != cola && c != colb) {
			D[rela][c] = D[rela][c]*(1-pow((1-float(F[rela])/pF),float(F[rela])/D[rela][c]));
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

int *Optimizer::best_plan(bool **predsJoined) {
	filter();		// TODO: does this belong elswhere?
	unordered_map<string, class JoinTree*> BestTree;
	JoinTree *currTree;
	for (int i = 0; i < nrel; i++) {
		string RIdStr(nrel, '0');
		RIdStr[i] = '1';
		// e.g. RIdStr for rel 2 with nrel=4 would be "0010" (relIds start from 0)
		currTree = new JoinTree(nrel, ncol[i], i, L[i], U[i], F[i], D[i], parser.npredicates);
		BestTree[RIdStr] = currTree;
	}
	for (int i = 1; i < nrel; i++) {
		string SIdStr(nrel - i, '0');
		SIdStr.append((unsigned int) i, '1');
		sort(SIdStr.begin(), SIdStr.end());
		do {	// get all treeIdStr permutations with exactly i '1's
			currTree = NULL;
			for (int j = 0; j < nrel; j++) {
				// if R[j] is included in S or is not to be joined with S, continue
				if ( SIdStr[j] == '1' || !connected(j, SIdStr) ) continue;
				currTree = new JoinTree(BestTree[SIdStr], j, L[j], U[j], F[j], D[j], parser);
				string newSIdStr = SIdStr;
				newSIdStr[j] = '1';
				// if there's no BestTree yet for newS or its cost is greater than currTree
				if ( BestTree.find(newSIdStr) == BestTree.end() || BestTree[SIdStr]->treeF < currTree->treeF) {
					BestTree[newSIdStr] = currTree;
				}
			}
			if (currTree == NULL) {		// No relations could be joined with currTree; CrossProducts are needed; Aborting best_plan()
				cout << "CrossProducts were found; Aborting BestTree!" << endl;
				// cleaning up BestTree:
				for (int ii = 1; ii <= i; ii++) {
					string iiSIdStr(nrel - ii, '0');
					iiSIdStr.append((unsigned int) ii, '1');
					sort(iiSIdStr.begin(), iiSIdStr.end());
					do {
						delete BestTree[iiSIdStr];
					} while ( next_permutation(iiSIdStr.begin(), iiSIdStr.end()) );
				}
				return NULL;
			}
		} while ( next_permutation(SIdStr.begin(), SIdStr.end()) );
	}
	int *bestJoinOrder = new int[nrel];
	string bestTreeIdStr(nrel, '1');
	currTree = BestTree[bestTreeIdStr];
	for (int i = 0; i < nrel; i++) {
		bestJoinOrder[i] = currTree->rowJoinOrder[i];
	}
	*predsJoined = new bool[parser.npredicates];
	for (int i = 0; i < parser.npredicates; i++) {
		*predsJoined[i] = currTree->predicatesJoined[i];
	}
	// cleaning up BestTree:
	for (int i = 1; i < nrel; i++) {
		string SIdStr(nrel - i, '0');
		SIdStr.append((unsigned int) i, '1');
		sort(SIdStr.begin(), SIdStr.end());
		do {
			delete BestTree[SIdStr];
		} while ( next_permutation(SIdStr.begin(), SIdStr.end()) );
	}
	return bestJoinOrder;
}
