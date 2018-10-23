#ifndef JOINRESULT_H
#define JOINRESULT_H


#include <iostream>
#include "FieldTypes.h"

using namespace std;

#define BUFFER_SIZE (1024*1024)    // buffer size for each ResultNode

struct ResultNode{
    intField buffer[BUFFER_SIZE / sizeof(intField)];
    ResultNode *next;
    ResultNode() : next(NULL) {}
};


class Result {
private:
    ResultNode *head;
public:
    Result() : head(NULL) {}
};

#endif
