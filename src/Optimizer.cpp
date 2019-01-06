#include <cmath>
#include <climits>
#include <unordered_map>
#include <algorithm>
#include "../Headers/macros.h"
#include "../Headers/Optimizer.h"

#define BIG_N 50000000

Optimizer::JoinTree::JoinTree(JoinTree *currBestTree, int relId, intField *relI, intField *relU, unsigned int relF,
		unsigned int *relD, SQLParser parser) : treeNrel(currBestTree->treeNrel) {
    rowJoinOrder = new int[currBestTree->treeNrel];
    rowJoinOrder[relId] = nextRelOrder;
    nextRelOrder++;
    int predicateJoined = calcJoinStats(parser, relId, relF, relD, &treeF, &treeD);
	CHECK( (predicateJoined > -1), "Error: tried to create Join Tree on non-connected realtions", );
	// TODO: pass parser by reference and update with predicateJoined
};

int Optimizer::JoinTree::calcJoinStats(SQLParser parser, int relId, unsigned int relF, unsigned int *relD,
                  unsigned int *newTreeF, unsigned int **newTreeD) {
    int predicateJoined = -1;
    int bestF = -1;
    unsigned int currF;
    int cola, colb;
    for (int i = 0; i < parser.npredicates; i++) {
        if (rowJoinOrder[parser.predicates[i].rela_id] > 0 && relId == parser.predicates[i].relb_id) {      // rela is in tree or is relId
            cola = parser.predicates[i].cola_id;
            colb = parser.predicates[i].colb_id;
            currF = (treeF * relF) / (treeU[cola] - treeI[cola] + 1);
            if (bestF == -1 || currF < bestF) {         // join at this column is the best join (yet)
                bestF = *newTreeF = currF;
                *newTreeD[cola] = (treeD[cola] * relD[colb]) / (treeU[cola] - treeI[cola] + 1);
                // TODO: update stats for the other columns:
                // d' = treeD[j] * (1 - pow((1 - *newTreeD[cola] / treeD[cola]), treeF / treeD[j]) )
                for (int j; j < ncola; j++) {
                	if (j == cola) continue;

                }
                for (int j; j < ncolb; j++) {
                	if (j == colb) continue;

                }
                predicateJoined = i;
            }
        } else if (rowJoinOrder[parser.predicates[i].relb_id] > 0 && relId == parser.predicates[i].rela_id) {
			// TODO: the above, but reversed
        }
    }
    return predicateJoined;
}

void Optimizer::initialize(unsigned int rid,unsigned int rows,unsigned int cols,intField **columns) {
	ncol[rid]=cols;
	I[rid]=new intField[cols];
	U[rid]=new intField[cols];
	F[rid]=new unsigned int[cols];
	D[rid]=new unsigned int[cols];
	N[rid]=new unsigned int[cols];
	bitmap[rid]=new uint64_t*[cols];
	
	for (unsigned int c=0; c<cols; c++) {
        intField u=0;
		intField i=ULLONG_MAX;
		
		for (unsigned int r=0; r<rows; r++) {
			i=min(i,columns[c][r]);
			u=max(u,columns[c][r]);
		}
		
		I[rid][c]=i;
		U[rid][c]=u;
		F[rid][c]=rows;
		D[rid][c]=0;
	
		N[rid][c]=MIN(U[rid][c]-I[rid][c],BIG_N);
		bitmap[rid][c]=new uint64_t[N[rid][c]/64+1]();
		
		for (unsigned int r=0; r<rows; r++) {
			unsigned int cell=(columns[c][r]-I[rid][c]) % N[rid][c];
			if (bitmap[rid][c][cell/64] & (1<<(cell%64))==0) {
				D[rid][c]++;
				bitmap[rid][c][cell/64]|=1<<(cell%64);
			}
		}
	}
}

void Optimizer::filter() {
	for (unsigned int f=0; f<parser.nfilters; f++) {
		char cmp=parser.filters[f].cmp;
		intField value=parser.filters[f].value;
		unsigned int rel=parser.filters[f].rel_id;
		unsigned int col=parser.filters[f].col_id;

		unsigned int pF=F[rel][col];
		
		if (cmp=='=') {
			I[rel][col]=value;
			U[rel][col]=value;
			unsigned int cell=value % N[rel][col];
			
			if (bitmap[rel][col][cell/64] & (1<<(cell%64))) {
				F[rel][col]=F[rel][col]/D[rel][col];
				D[rel][col]=1;
			}
			else D[rel][col]=F[rel][col]=0;
		}
		
		if (cmp=='<') {
			if (value-1>=U[rel][col]) continue;
			U[rel][col]=value-1;
			D[rel][col]=D[rel][col]*double(value-1-I[rel][col])/(U[rel][col]-I[rel][col]);
			F[rel][col]=F[rel][col]*double(value-1-I[rel][col])/(U[rel][col]-I[rel][col]);
		}
			
		if (cmp=='>') {
			if (value+1<=I[rel][col]) continue;
			I[rel][col]=value+1;
			D[rel][col]=D[rel][col]*double(-value-1+U[rel][col])/(U[rel][col]-I[rel][col]);
			F[rel][col]=F[rel][col]*double(-value-1+U[rel][col])/(U[rel][col]-I[rel][col]);
		}
		
		for (unsigned int c=0; c<ncol[rel]; c++) if (c!=col) {
			D[rel][c]=D[rel][c]*(1-pow((1-float(F[rel][col])/pF),float(F[rel][c])/D[rel][c]));
			F[rel][c]=F[rel][col];
		}
	}
		
	for (unsigned int p=0; p<parser.npredicates; p++) {
		unsigned int rela=parser.predicates[p].rela_id;
		unsigned int relb=parser.predicates[p].relb_id;
		if (rela!=relb) continue;
		
		unsigned int cola=parser.predicates[p].cola_id;
		unsigned int colb=parser.predicates[p].colb_id;
		
		unsigned int pF=F[rela][cola];
		
		I[rela][cola]=I[rela][colb]=max(I[rela][cola],I[rela][colb]);
		U[rela][cola]=U[rela][colb]=min(U[rela][cola],U[rela][colb]);
		F[rela][cola]=F[rela][colb]=F[rela][cola]/(U[rela][cola]-I[rela][cola]+1);
		D[rela][cola]=D[rela][colb]=D[rela][cola]*(1-pow((1-float(F[rela][cola])/pF),float(pF)/D[rela][cola]));
		
		for (unsigned int c=0; c<ncol[rela]; c++) if (c!=cola && c!=colb) {
			D[rela][c]=D[rela][c]*(1-pow((1-float(F[rela][cola])/pF),float(F[rela][c])/D[rela][c]));
			F[rela][c]=F[rela][cola];
		}
	}
}

//bool ctob(char boolChar) {
//	return (boolChar == '1');
//}
//
//char btoc(bool charBool) {
//	return (charBool) ? '1' : '0';
//}
//
//string getCombinedIdStr(string str1, string str2) {
//	string newRelId;
//	for (int i = 0; i < str1.length(); i++) {
//		newRelId += btoc(ctob(str1[i]) | ctob(str2[i]));
//	}
//	return newRelId;
//}

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
	filter();		// TODO: does this belong elswhere?
	unordered_map<string, class JoinTree*> BestTree;
	JoinTree *currTree;
	for (int i = 0; i < nrel; i++) {
		string RIdStr(nrel, '0');
		RIdStr[i] = '1';
		// e.g. RIdStr for rel 2 with nrel=4 would be "0010" (relIds start from 0)
		currTree = new JoinTree(nrel, ncol[i], i, I[i], U[i], F[i][0], D[i]);
		BestTree[RIdStr] = currTree;
	}
	for (int i = 1; i < nrel; i++) {
		string SIdStr(nrel - i, '0');
		SIdStr.append((unsigned int) i, '1');
		sort(SIdStr.begin(), SIdStr.end());
		do {	// get all treeIdStr permutations with exactly i '1's
			for (int j = 0; j < nrel; j++) {
				// if R[j] is included in S or is not to be joined with S, continue
				if ( SIdStr[j] == '1' || !connected(j, SIdStr) ) continue;
				currTree = new JoinTree(BestTree[SIdStr], j, I[j], U[j], F[j][0], D[j], parser);
				string newSIdStr = SIdStr;
				newSIdStr[j] = '1';
				// if there's no BestTree yet for newS or its cost is greater than currTree
				if ( BestTree.find(newSIdStr) == BestTree.end() || BestTree[SIdStr]->treeF < currTree->treeF) {
					BestTree[newSIdStr] = currTree;
				}
			}
		} while ( next_permutation(SIdStr.begin(), SIdStr.end()) );
	}
	int *bestJoinOrder = new int[nrel];
	string bestTreeIdStr(nrel, '1');
	currTree = BestTree[bestTreeIdStr];
	for (int i = 0; i < nrel; i++) {
		bestJoinOrder[i] = currTree->rowJoinOrder[i];
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
