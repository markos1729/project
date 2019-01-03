#ifndef OPTIMIZER_H
#define OPTIMIZER_H

#include <vector>
#include "SQLParser.h"
#include "FieldTypes.h"
using namespace std;

class Optimizer {
	private:
		unsigned int nrel;  //number of relations
		unsigned int *ncol; //number of columns for each relation
				
		intField **I;     //minimum value for each column
		intField **U;     //maximum value for each column
		unsigned int **F; //number of rows for each column
		unsigned int **D; //number of distinct values for each column

		SQLParser parser;   //parser for this query
		unsigned int **N;   //bitmap size for each column
		uint64_t ***bitmap; //compact bitmap for each column

		void filter();

	public:
		Optimizer(unsigned int _nrel,SQLParser _parser) : nrel(_nrel), parser(_parser) {
			ncol=new unsigned int[nrel];
			I=new intField*[nrel];
			U=new intField*[nrel];
			F=new unsigned int*[nrel];
			D=new unsigned int*[nrel];
			N=new unsigned int*[nrel];
			bitmap=new uint64_t**[nrel];
			}
			
		~Optimizer() {
			for (unsigned int r=0; r<nrel; r++) {
				delete[] I[r];
				delete[] U[r];
				delete[] F[r];
				delete[] D[r];
				delete[] N[r];
				delete I;
				delete U;
				delete F;
				delete D;
				delete N;
				for (unsigned int c=0; c<ncol[r]; c++) delete[] bitmap[r][c];
				delete bitmap[r];
				delete bitmap;
				}
			delete[] ncol;
			}
		
		void initialize(unsigned int rid,unsigned int rows,unsigned int cols,intField **columns);
		void best_plan();
	};

#endif
