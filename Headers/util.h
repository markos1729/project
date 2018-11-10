#include <iostream>
#include <string.h>

using namespace std;

class CString_List {
    struct Node{
    	char *str;
    	Node *next;
    	Node(const char *s) : next(NULL) {
    		str = new char[strlen(s) + 1];
    		strcpy(str, s);
    	}
    	// (!) Destructor should NOT delete str from heap as we want it returned in pop()
    } *head, *tail;
    unsigned int size;
public:
	CString_List() : head(NULL), tail(NULL), size(0) {}
	~CString_List();
	unsigned int getSize() const { return size; }
	void append(const char *str);
	char *pop();         // (!) user has the responsibility of "delete[]"ing popped string from heap
};
