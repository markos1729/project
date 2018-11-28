#include <iostream>
#include <string.h>
#include <unistd.h>
#include "Headers/util.h"
#include "Headers/Relation.h"
#include "Headers/RadixHashJoin.h"
#include "Headers/SQLParser.h"


using namespace std;


#define CHECK(call, msg, actions) { if ( !(call) ) { cerr << msg << endl; actions } }


#define MAX_FILE_NAME_SIZE 1024
#define MAX_QUERY_SIZE 4096


/* Local Functions */
unsigned int find_rel_pos(QueryRelation *QueryRelations, unsigned int size, unsigned int rel_id);


int main(){
	// first read line-by-line for relations' file names until read "DONE"
	CString_List fileList;
	{	// do this in a block so that the buffer will be free-ed up afterwards from stack
		char buffer[MAX_FILE_NAME_SIZE];
		cin.getline(buffer, MAX_FILE_NAME_SIZE);
		while ( !cin.fail() && strcmp(buffer, "DONE") != 0 ){
			fileList.append(buffer);
			cin.getline(buffer, MAX_FILE_NAME_SIZE);
		}
	}
	CHECK( !cin.fail() , "Error: an input line was too long", return -1; )
	// and load all files into memory
	const unsigned int number_of_relations = fileList.getSize();
	Relation **R = new Relation *[number_of_relations]();
	unsigned int i;
	char *filename;
	for (i = 0 ; i < number_of_relations && (filename = fileList.pop()) != NULL ; i++ ){
		R[i] = new Relation(filename, i);
		delete[] filename;
	}
	CHECK( i == number_of_relations, "Warning: Unexpected number of relations", )   // should not happen
	// wait for 1 second
	sleep(1);
	// then start parsing 'sql' statements
	char buffer[MAX_QUERY_SIZE];
	while (!cin.eof() && !cin.fail()){
		cin.getline(buffer, MAX_QUERY_SIZE);
		SQLParser *p = new SQLParser(buffer);     // example: "0 2 4|0.1=1.2&1.0=2.1&0.1>3000|0.0 1.1";

		//DEBUG
		p->show();

		bool abort = false;

		// execute FROM: load original Relations to an array of pointers to such (which will only get "smaller" during the query)
		QueryRelation **QueryRelations = new QueryRelation*[p->nrelations]();
		for (int i = 0; i < p->nrelations ; i++){
			CHECK( p->relations[i] < number_of_relations, "SQL Error: SQL query contains non-existant Relation in \'FROM\'. Aborting query...", delete[] QueryRelations; delete p; abort = true; break; )
			QueryRelations[i] = R[p->relations[i]];
		}
		if (abort) continue;

		// execute WHERE
		// First, do filters
		for (int i = 0 ; i < p->nfilters ; i++){
			//FILTER
			const filter &filter = p->filter[i];
			CHECK( filter.rel_id < p->nrelations, "SQL Error: SQL filter contains a relation that does not exist in \'FROM\'. Aborting query...", 
				   for (int i = 0 ; i < p->nrelations; i++) { if ( p->isIntermediate ) delete QueryRelations[i]; } delete[] QueryRelations; delete p; abort = true; break; )
			QueryRelations[filter.rel_id] = QueryRelations[filter.rel_id]->performFilter(filter.col_id, filter.value, filter.cmp);
		}
		if (abort) continue;
		// Then equal columns operations
		for (int i = 0 ; i < p->npredicates ; i++){
			if ( p->predicates[i].rela_id == p->predicates[i].relb_id && p->predicates[i].cola_id != p->predicates[i].colb_id){
				// EQUAL COLUMNS UNARY OPERATION
				const predicate &predicate = p->predicates[i];
				CHECK( predicate.rela_id < p->nrelations, "SQL Error: SQL equal columns predicate on one relation that does not exist in \'FROM\'. Aborting query...", 
					   for (int i = 0 ; i < p->nrelations; i++) { if ( p->isIntermediate ) delete QueryRelations[i]; } delete[] QueryRelations; delete p; abort = true; break;)
				QueryRelations[predicate.rela_id] = QueryRelations[predicate.rela_id]->performEqColumns(predicate.cola_id, predicate.colb_id);
			}
			// else if (  p->predicates[i].rela_id == p->predicates[i].relb_id  ) -> ignore predicate
		}
		if (abort) continue;
		// And afterwards all the joins
		for (int i = 0 ; i < p->npredicates ; i++){
			CHECK( predicate.rela_id < p->nrelations && predicate.relb_id < p->nrelations, "SQL Error: SQL join predicate contains a relation that does not exist in \'FROM\'. Aborting query...", 
				   for (int i = 0 ; i < p->nrelations; i++) { if ( p->isIntermediate ) delete QueryRelations[i]; } delete[] QueryRelations; delete p; abort = true; break; )
			const predicate &predicate = p->predicates[i];
			unsigned int rela_pos = find_rel_pos(QueryRelations, p->nrelations, predicate.rela_id);
			unsigned int relb_pos = find_rel_pos(QueryRelations, p->nrelations, predicate.relb_id);
			CHECK( rela_pos != -1 && relb_pos != -1, "Warning: Something went wrong joining relations, cannot find intermediate for one. Please debug.", continue; )
			if ( rela_pos != relb_pos ){
				// JOIN
				if (rela_pos < relb_pos){
					QueryRelations[rela_pos] = QueryRelations[rela_pos]->performJoinWith(QueryRelations[relb_pos], predicate.cola_id, predicate.colb_id);
					if ( QueryRelations[relb_pos]->isIntermediate ) delete QueryRelations[relb_pos];
					QueryRelations[relb_pos] = NULL;
				} else {
					QueryRelations[relb_pos] = QueryRelations[relb_pos]->performJoinWith(QueryRelations[rela_pos], predicate.colb_id, predicate.cola_id);
					if ( QueryRelations[rela_pos]->isIntermediate ) delete QueryRelations[rela_pos];
					QueryRelations[rela_pos] = NULL;
				}
			} else {   // (!) if two tables are joined two times then the first it will be join whilst the second time it will be an equal columns operation!
				// EQUAL COLUMNS UNARY OPERATION
				QueryRelations[rela_pos] = QueryRelations[rela_pos]->performEqColumns(predicate.cola_id, predicate.colb_id);
			}
		}
		if (abort) continue;
		CHECK( QueryRelations[0] != NULL, "Warning: Something not supposed to happen happened. Please debug...", 
			   for (int i = 0 ; i < p->nrelations; i++) { if ( p->isIntermediate ) delete QueryRelations[i]; } delete[] QueryRelations; delete p; break; )
		// Last but not least any cross-products left to do
		while ( count_not_null(QueryRelations, p->nrelations) > 1 ){
			// CROSS PRODUCT: perform cross product between remaining Relations uniting them into one in the leftest position until only one QueryRelation remains
			int i = 1;
			while ( i < p->nrelations && QueryRelations[i] == NULL ) i++;
			if ( i == p->nrelations) { cerr << "Warning: should not happen" << endl; break; }
			QueryRelations[0] = QueryRelations[0]->performCrossProductWith(QueryRelations[i]);
			if (QueryRelations[i]->isIntermediate) delete QueryRelations[i];
			QueryRelations[i] = NULL;
		}

		// execute SELECT : not SUM yet -> TODO change later after it's working
		R[0]->performSelect(R, p->projections, p->nprojections);

		// cleanup
		for (int i = 0 ; i < p->nrelations; i++){
			if ( p->isIntermediate ) delete QueryRelations[i]; 
		}
		delete[] QueryRelations;
		delete p;
	}
	// cleanup
	for (i = 0 ; i < number_of_relations ; i++ ){
		//DEBUG: cout << R[i]->getNumOfColumns() << endl;
		delete R[i];
	}
	delete[] R;
	return 0;
}


/* Local Function Implementation */
unsigned int find_rel_pos(QueryRelation *QueryRelations, unsigned int size, unsigned int rel_id){
	for (int i = 0 ; i < size ; i++){
		if (QueryRelations[i] != NULL && QueryRelations[i]->containsRelation(rel_id)){
			return i;
		}
	}
	return -1;  // unsigned -> 111..1 -> should be caught by a CHECK(..)
}
