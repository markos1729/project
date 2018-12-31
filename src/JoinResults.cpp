#include <stdio.h>
#include "../Headers/JoinResults.h"
#include "../Headers/macros.h"


/* ResultNode Implementation */
ResultNode::ResultNode() : nextpos(0), next(NULL) {}

bool ResultNode::isFull() {    // can not fit one more tuple
    return nextpos > (BUFFER_SIZE / sizeof(unsigned int) - 2);
}

bool ResultNode::addTuple(unsigned int val1, unsigned int val2) {
    if (nextpos >= BUFFER_SIZE / sizeof(unsigned int) ) return false;
    buffer[nextpos] = val1;
    buffer[nextpos+1] = val2;
    nextpos += 2;
    return true;
}

#ifdef DDEBUG
void ResultNode::printRowIds() {
    printf("| RowIdR | RowIdS |\n");
    for (unsigned int pos = 0; pos < nextpos; pos += 2) {
        printf("| %6u | %6u |\n", buffer[pos], buffer[pos + 1]);
    }
    printf("\n");
}
#endif

/* Result Implementation */
Result::Result() : head(NULL), cur(NULL), size(0) {
}

Result::~Result() {
    ResultNode *temp = head;
    while (temp != NULL){
        ResultNode *next = temp->next;
        delete temp;
        temp = next;
    }
}

bool Result::addTuple(unsigned int rowid1, unsigned int rowid2) {
    bool ret_val = false;
    if (head == NULL){
        head = new ResultNode;
        cur = head;
        if ( head->addTuple(rowid1, rowid2) ){
        	size++;
        	ret_val = true;
        }
    } else {
        if ( ! cur->isFull() ){
            if ( cur->addTuple(rowid1, rowid2) ){
            	size++;
        	    ret_val = true;
            }
        } else {
            ResultNode *temp = cur;
            cur = new ResultNode;
            temp->next = cur;
            if ( cur->addTuple(rowid1, rowid2) ){
            	size++;
                ret_val = true;
            }
        }
    }
    return ret_val;
}

void Result::addList(Result *r2) {
    if (r2 == NULL) {
        cerr << "Warning: NULL r2 result in Result::addList()" << endl;
        return;
    }
    if ( (cur == NULL || head == NULL || size == 0 ) && (r2->cur == NULL || r2->head == NULL || r2->size == 0 )) {
        // do nothing - nothing to append + result must remain NULL
        return;
    } else if (cur == NULL || head == NULL || size == 0 ) {
        head = r2->head;
        cur = r2->cur;
        size = r2->size;
    } else if (r2->cur == NULL || r2->head == NULL || r2->size == 0 ) {
        // do nothing - nothing to append
        return;
    } else {
        // append r2 to current Result list
        cur->next = r2->head;
        cur = r2->cur;
        size += r2->size;
    }
    // "empty" r2  (!) Important so that r2's destructor does not kill everything we appended
    r2->head = NULL;
    r2->cur = NULL;
    r2->size = 0;
}

#ifdef DDEBUG
void Result::printRowIds() {
    ResultNode *current = head;
    while (current != NULL) {
        current->printRowIds();
        current = current->next;
    }
}
#endif

/* Iterator Implementation */
bool Iterator::getNext(unsigned int &rid, unsigned int &sid) {
    if (curr == NULL) return false;
    rid = curr->buffer[pos];
    sid = curr->buffer[pos+1];
    pos += 2;
    if (pos >= curr->nextpos) { 
	    curr = curr->next;
        pos = 0;  // reset pos to 0
    }
    return true;
}
