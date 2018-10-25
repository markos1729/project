#ifndef JOINRESULT_H
#define JOINRESULT_H


#include <iostream>
#include "FieldTypes.h"

using namespace std;


#define BUFFER_SIZE (1024*1024)    // buffer size for each ResultNode


struct ResultNode{
    private:
        unsigned int buffer[BUFFER_SIZE / sizeof(unsigned int)];
        int nextpos;
    public:
        ResultNode *next;
        ResultNode();
        bool isFull();
        bool addTuple(unsigned int val1, unsigned int val2);
};


class Result {
private:
    ResultNode *head;
    ResultNode *cur;
public:
    Result();
    bool addTuple(unsigned int rowid1, unsigned int rowid2);
};

#endif
