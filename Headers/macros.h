#ifndef PROJECT_MACROS_H
#define PROJECT_MACROS_H

#include <iostream>

#define CHECK(call, msg, action) { if ( ! (call) ) { std::cerr << msg << std::endl; action } }
#define CHECK_PERROR(call, msg, actions) { if ( (call) < 0 ) { perror(msg); actions } }

#define MAX(A, B) ( (A) > (B) ? (A) : (B) )
#define MIN(A, B) ( (A) < (B) ? (A) : (B) )

#endif
