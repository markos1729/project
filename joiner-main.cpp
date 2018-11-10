#include <iostream>
#include <string.h>
#include <unistd.h>
#include "Headers/util.h"
#include "Headers/Relation.h"
#include "Headers/RadixHashJoin.h"

using namespace std;


#define CHECK(call, msg, actions) { if ( !(call) ) { cerr << msg << endl; actions } }


#define MAX_FILE_NAME_SIZE 1024


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
	
	//TODO
	
	// cleanup
	for (i = 0 ; i < number_of_relations ; i++ ){
		//DEBUG: cout << R[i]->getNumOfColumns() << endl;
		delete R[i];
	}
	delete[] R;
	return 0;
}
