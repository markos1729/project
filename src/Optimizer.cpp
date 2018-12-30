#include <math.h>
#include <limits.h>
#include "../Headers/Optimizer.h"

#define BIG_N 50000000
#define max(x,y) (x>y ? x : y)
#define min(x,y) (x<y ? x : y)

void Optimizer::initialize(unsigned int rid,unsigned int rows,unsigned int cols,intField **columns) {
	ncol[rid]=cols;
	I[rid]=new intField[cols];
	U[rid]=new intField[cols];
	F[rid]=new unsigned int[cols];
	D[rid]=new unsigned int[cols];
	N[rid]=new unsigned int[cols];
	bitmap[rid]=new unsigned int*[cols];
	
	for (unsigned int c=0; c<cols; c++) {
		intField i=ULLONG_MAX;
		intField u=ULLONG_MIN;
		
		for (unsigned int r=0; r<rows; r++) {
			i=min(i,columns[c][r]);
			u=max(u,columns[c][r]);
			}
		
		I[rid][c]=i;
		U[rid][c]=u;
		F[rid][c]=rows;
		D[rid][c]=0;
	
		N[rid][c]=min(U[rid][c]-I[rid][c],BIG_N);
		bitmap[rid][c]=new uint64_t[N[rid][c]/64+1]();
		
		for (unsigned int r=0; r<rows; r++) {
			unsigned int cell=(columns[c][r]-I[rid][c]) % N[rid][c];
			if (bitmap[rid][cell/64] & (1<<(cell%64))==0) {
				D[rid][c]++;
				bitmap[rid][cell/64]|=1<<(cell%64);
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
		
		unsigned int pF=F[rel][cola];
		
		I[rela][cola]=I[rela][colb]=max(I[rela][cola],I[rela][colb]);
		U[rela][cola]=U[rela][colb]=min(U[rela][cola],U[rela][colb]);
		F[rela][cola]=F[rela][colb]=F[rel][cola]/(U[rel][cola]-I[rel][cola]+1);
		D[rela][cola]=D[rela][colb]=D[rela][cola]*(1-pow((1-float(F[rela][cola])/pF),float(pF)/D[rela][cola]));
		
		for (unsigned int c=0; c<ncol[rela]; c++) if (c!=cola && c!=colb) {
			D[rela][c]=D[rela][c]*(1-pow((1-float(F[rel][cola])/pF),float(F[rela][c])/D[rela][c]));
			F[rela][c]=F[rela][cola];
			}
		}
	}

void Optimizer::best_plan() {
	}
