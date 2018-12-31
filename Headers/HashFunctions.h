#ifndef PROJECT_HASHFUNCTIONS_H
#define PROJECT_HASHFUNCTIONS_H


inline unsigned int H1(intField value, unsigned int H1_N){
    intField mask = 0;
    for (unsigned int i = 0 ; i < H1_N ; i++ ){
        mask |= 0x01<<i;
    }
    return (unsigned int) (mask & value);
}


inline unsigned int H2(intField value, unsigned int H1_N, unsigned int H2_N) {
    return ( value & ( ((1 << (H1_N + H2_N)) - 1) ^ ( (1 << H1_N) - 1) ) ) >> H1_N;
}


#endif
