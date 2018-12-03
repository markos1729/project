#include <iostream>
#include "../Headers/util.h"


using namespace std;


unsigned int count_not_null(void **ptrarray, unsigned int size){
	unsigned int count = 0;
	for (unsigned int i = 0 ; i < size ; i++){
		if ( ptrarray[i] != NULL ) count++;
	}
	return count;
}
