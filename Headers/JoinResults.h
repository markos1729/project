#ifndef JOINRESULTS_H
#define JOINRESULTS_H


#include <iostream>
#include "FieldTypes.h"

//#define DDEBUG                     // define this if functions used for debugging such as printing info should be compiled

#define BUFFER_SIZE (1024*1024)    // buffer size for each ResultNode


using namespace std;


struct ResultNode{
	friend class Iterator;
    private:
        unsigned int nextpos;
        unsigned int buffer[BUFFER_SIZE / sizeof(unsigned int)];
    public:
        ResultNode *next;
        ResultNode();
        bool isFull();
        bool addTuple(unsigned int val1, unsigned int val2);
        #ifdef DDEBUG
        void printRowIds();
        #endif
};


class Result {
	friend class Iterator;
	private:
    		unsigned long long int size;
		ResultNode *head;
		ResultNode *cur;
	public:
		Result();
		~Result();
		unsigned long long int getSize() const { return size; }
		bool addTuple(unsigned int rowid1, unsigned int rowid2);
		#ifdef DDEBUG
		void printRowIds();
		#endif
};

class Iterator {
	private:
		unsigned int pos;
		ResultNode *curr;
	public:
		Iterator(Result *r) : pos(0), curr(r->head) {}
		bool getNext(unsigned int &rid, unsigned int &sid);
};

#endif
