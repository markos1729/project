#ifndef JOINRESULTS_H
#define JOINRESULTS_H


#include <iostream>
#include "FieldTypes.h"

using namespace std;


#define BUFFER_SIZE (1024*1024)    // buffer size for each ResultNode


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
        void printRowIds();
};


class Result {
	friend class Iterator;
	private:
		ResultNode *head;
		ResultNode *cur;
	public:
		Result();
		~Result();
		bool addTuple(unsigned int rowid1, unsigned int rowid2);
		void printRowIds();
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
