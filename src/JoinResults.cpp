#include "../Headers/JoinResults.h"

ResultNode::ResultNode() : next(NULL), nextpos(0) {}

ResultNode *ResultNode::getNext() const {
    return next;
}

void ResultNode::setNext(ResultNode *next) {
    ResultNode::next = next;
}

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
    cout << "| RowIdR | RowIdS |" << endl;
    for (int pos = 0; pos < nextpos; pos += 2) {
        cout << "| " << buffer[pos] << " | " << buffer[pos + 1] << " |" << endl;
    }
    cout << endl;
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
        } else {
            ResultNode *temp = cur;
            cur = new ResultNode;
            temp->setNext(cur);
            return cur->addTuple(rowid1, rowid2);
        }
    }
}

void Result::printRowIds() {
    ResultNode *current = head;
    while (current != NULL) {
        current->printRowIds();
        current = current->getNext();
    }
}
