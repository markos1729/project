#ifndef PROJECT_CONFIGUREPARAMETERS_H
#define PROJECT_CONFIGUREPARAMETERS_H


///////////////////////////////
/// JOINER-RHJ  PARAMETERS  ///
///////////////////////////////

#define CACHE 512                  // expected average bucket size: smaller CACHE SIZE means more buckets (=> more parallel joins but bigger parallel partition cost)
#define NUMBER_OF_THREADS 8        // joiner's scheduler thread pool size
#define CPU_CORES 8                // this will be taken into account when chunking bigger arrays into parts
#define BUFFER_SIZE 128000         // buffer size for result nodes. Recommended: 128KB


///////////////////////////////
/// CHOOSE PARALLEL OPTIONS ///
///////////////////////////////

//#define PARALLEL_IMPLEMENTATION_OF_PARTITION_INSTEAD_OF_THEIR_CALL   // if #defined it seems to be slower
#define DO_FILTER_PARALLEL
#define DO_EQUALCOLUMNS_PARALLEL


///////////////////////////////
///   COMPILE DEBUG CODE?   ///
///////////////////////////////

//#define DDEBUG


#endif
