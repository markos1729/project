#ifndef relation_h
#define relation_h

#include <stdint.h>

class relation {
	private:
		int size,H1,H2;
		uint32_t *column;
		int **chain,**bucket;

	public:
		relation(int,uint32_t*);
		~relation();
		void build(uint32_t,uint32_t);
};

#endif
