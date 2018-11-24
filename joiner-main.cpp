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
		R[i] = new Relation(filename);
		delete[] filename;
	}
	CHECK( i == number_of_relations, "Warning: Unexpected number of relations", )   // should not happen
	// wait for 1 second
	sleep(1);
	// then start parsing 'sql' statements
	char buffer[MAX_QUERY_SIZE];
	while (!cin.eof() && !cin.fail()){
		cin.getline(buffer, MAX_FILE_NAME_SIZE);
		SQLParser *p = new SQLParser(buffer);     // example: "0 2 4|0.1=1.2&1.0=2.1&0.1>3000|0.0 1.1";

		//DEBUG
		p->show();
		
		//TODO: execute query
		// Arbitrary order: Do filters first, predicates next (query optimization will be part of the next assignment)
		for (int i = 0 ; i < p->nfilters ; i++){
			//FILTER
			//TODO
		}
		for (int i = 0 ; i < p->npredicates ; i++){
			if ( p->predicates[i].rela_id == p->predicates[i].relb_id && p->predicates[i].cola_id != p->predicates[i].colb_id){
			// EQUAL COLUMNS UNARY OPERATION
			//TODO
			} 
			else if ( p->predicates[i].rela_id != p->predicates[i].relb_id ){
			// JOIN
			//TODO
			} 
			//else same relation and same columns -> ignore operation
		}

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
