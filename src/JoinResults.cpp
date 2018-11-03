#include <stdio.h>
#include "../Headers/JoinResults.h"


ResultNode::ResultNode() : next(NULL), nextpos(0) {}

bool ResultNode::isFull() {    // can not fit one more tuple
    return nextpos > (BUFFER_SIZE / sizeof(unsigned int) - 2);
}

bool ResultNode::addTuple(unsigned int val1, unsigned int val2) {
    if (nextpos < 0 || nextpos >= BUFFER_SIZE / sizeof(unsigned int) ) return false;
    buffer[nextpos] = val1;
    buffer[nextpos+1] = val2;
    nextpos += 2;
    return true;
}

void ResultNode::printRowIds() {
    printf("| RowIdR | RowIdS |\n");
    for (int pos = 0; pos < nextpos; pos += 2) {
        printf("| %6u | %6u |\n", buffer[pos], buffer[pos + 1]);
    }
    printf("\n");
}

Result::Result() : head(NULL), cur(NULL) {}

Result::~Result() {
    ResultNode *temp = head;
    while (temp != NULL){
        ResultNode *next = temp->next;
        delete temp;
        temp = next;
    }
}

bool Result::addTuple(unsigned int rowid1, unsigned int rowid2) {
    if (head == NULL){
        head = new ResultNode;
        cur = head;
        return head->addTuple(rowid1, rowid2);
    } else {
        if ( ! cur->isFull() ){
            return cur->addTuple(rowid1, rowid2);
        } else {
            ResultNode *temp = cur;
            cur = new ResultNode;
            temp->next = cur;
            return cur->addTuple(rowid1, rowid2);
        }
    }
}

void Result::printRowIds() {
    ResultNode *current = head;
    while (current != NULL) {
        current->printRowIds();
        current = current->next;
    }
}

bool Iterator::getNext(unsigned int &rid, unsigned int &sid) {
	if (curr == NULL) return false;
    rid = curr->buffer[pos];
    sid = curr->buffer[pos+1];
    pos += 2;
	if (pos >= curr->nextpos) curr = curr->next;
    return true;
}
