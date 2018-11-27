#include <iostream>
#include "../Headers/util.h"


using namespace std;


CString_List::~CString_List() {
	char *str;
	while ( (str = pop()) != NULL ){
		delete[] str;
	}
}

void CString_List::append(const char *str) {
	size++;
	if (head == NULL) {
		head = tail = new Node(str);
	} else if (head == tail){
		tail = head->next = new Node(str);
	} else {
		tail->next = new Node(str);
		tail = tail->next;
	}
}
char *CString_List::pop() {          // (!) user has the responsibility of "delete[]"ing popped string from heap
	if (head == NULL) {
		return NULL;
	} else if (head == tail) {
		size--;
		char *rval = head->str;
		delete head;
		head = tail = NULL;
		return rval;
	} else {
		size--;
		Node *temp = head;
		char *rval = head->str;
		head = head->next;
		delete temp;
		return rval;
	}
}

unsigned int count_not_null(void **ptrarray, unsigned int size){
	unsigned int count = 0;
	for (unsigned int i = 0 ; i < size ; i++){
		if ( ptrarray[i] != NULL ) count++;
	}
	return count;
}
