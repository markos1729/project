
// useful macros for checks:
#define CHECK(call, msg, actions) { if ( !(call) ) { std::cerr << msg << std::endl; actions } }

unsigned int count_not_null(void **ptrarray, unsigned int size);
