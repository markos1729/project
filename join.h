#ifndef join_h
#define join_h

#include <stdint.h>
#include "relation.h"

class result {
	private:
	
	public:
		uint32_t* get(int);
};

result join(relation&,relation&);

#endif
