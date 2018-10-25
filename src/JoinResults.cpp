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

Result::Result() : head(NULL), cur(NULL) {}

bool Result::addTuple(unsigned int rowid1, unsigned int rowid2) {
    if (head == NULL){
        head = new ResultNode;
        cur = head;
        return head->addTuple(rowid1, rowid2);
    } else {
        if ( ! cur->isFull() ){
            return cur->addTuple(rowid1, rowid2);
        } else{
            ResultNode *temp = cur;
            cur = new ResultNode;
            temp->next = cur;
            return cur->addTuple(rowid1, rowid2);
        }
    }
}
