#ifndef JOINRESULT_H
#define JOINRESULT_H


#include <iostream>
#include <stdint-gcc.h>

using namespace std;

#define BUFFER_SIZE (1024*1024)    // buffer size for each ResultNode

struct ResultNode{
    uint32_t buffer[BUFFER_SIZE / sizeof(uint32_t)];
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
