#include "Headers/RadixHashJoin.h"
#include <stdio.h>

#define try(a) fprintf(stderr,"%s\n",#a);a();
#define assert(a) if (!(a)) fprintf(stderr,"assert %s failed\n",#a);

void test_index() {
	assert(0==1);
	assert(2==2);
	assert(3==2);
}

int main() {
	try(test_index);		
	return 0;
}
