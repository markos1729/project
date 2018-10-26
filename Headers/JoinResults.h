#ifndef JOINRESULTS_H
#define JOINRESULTS_H


#include <iostream>
#include "FieldTypes.h"

using namespace std;


#define BUFFER_SIZE (1024*1024)    // buffer size for each ResultNode


struct ResultNode{
    private:
        ResultNode *next;
        unsigned int buffer[BUFFER_SIZE / sizeof(unsigned int)];
        int nextpos;
    public:
        ResultNode();
        ResultNode *getNext() const;
        void setNext(ResultNode *next);
        bool isFull();
        bool addTuple(unsigned int val1, unsigned int val2);
        void printRowIds();
};


class Result {
private:
    ResultNode *head;
    ResultNode *cur;
public:
    Result();
    bool addTuple(unsigned int rowid1, unsigned int rowid2);
    void printRowIds();
};

#endif
