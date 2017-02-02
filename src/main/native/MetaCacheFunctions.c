#include "com_github_metacachespark_HashFunctions.h"
#include <string.h>

inline unsigned long make_reverse_complement64C(unsigned long s, unsigned char k) {
    s = ((s >> 2)  & 0x3333333333333333ul) | ((s & 0x3333333333333333ul) << 2);
    s = ((s >> 4)  & 0x0F0F0F0F0F0F0F0Ful) | ((s & 0x0F0F0F0F0F0F0F0Ful) << 4);
    s = ((s >> 8)  & 0x00FF00FF00FF00FFul) | ((s & 0x00FF00FF00FF00FFul) << 8);
    s = ((s >> 16) & 0x0000FFFF0000FFFFul) | ((s & 0x0000FFFF0000FFFFul) << 16);
    s = ((s >> 32) & 0x00000000FFFFFFFFul) | ((s & 0x00000000FFFFFFFFul) << 32);
    return ((unsigned long)(-1) - s) >> (8 * sizeof(s) - (k << 1));
}

//-------------------------------------------------------------------
inline unsigned int make_reverse_complement32C(unsigned int s, unsigned char k) {
    s = ((s >> 2)  & 0x33333333u) | ((s & 0x33333333u) << 2);
    s = ((s >> 4)  & 0x0F0F0F0Fu) | ((s & 0x0F0F0F0Fu) << 4);
    s = ((s >> 8)  & 0x00FF00FFu) | ((s & 0x00FF00FFu) << 8);
    s = ((s >> 16) & 0x0000FFFFu) | ((s & 0x0000FFFFu) << 16);
    return ((unsigned int)(-1) - s) >> (8 * sizeof(s) - (k << 1));
}

inline unsigned int thomas_mueller_hash(unsigned int x) {
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x);
    return x;
}


inline unsigned int kmer2uint32C(const char *input) {


    int i = 0;
    unsigned int kmer = 0x00000000u;

    for(i=0; i< strlen(input);i++) {

        kmer <<= 2;

        switch(input[i]) {
            case 'A': case 'a': break;
            case 'C': case 'c': kmer |= 1; break;
            case 'G': case 'g': kmer |= 2; break;
            case 'T': case 't': kmer |= 3; break;
            default: break;
        }

    }

    return kmer;


}


JNIEXPORT jlong JNICALL Java_com_github_metacachespark_HashFunctions_make_1reverse_1complement64 (JNIEnv *env, jclass thisObj, jlong s, jint k) {

    unsigned long newS = (unsigned long) s;
    unsigned char newK = (unsigned char) k;

    return make_reverse_complement64C(newS, newK);


}

JNIEXPORT jint JNICALL Java_com_github_metacachespark_HashFunctions_make_1reverse_1complement32 (JNIEnv *env, jclass thisObj, jint s, jint k) {

    unsigned int newS = (unsigned int) s;
    unsigned char newK = (unsigned char) k;

    return make_reverse_complement32C(newS, newK);

}

JNIEXPORT jint JNICALL Java_com_github_metacachespark_HashFunctions_thomas_1mueller_1hash32  (JNIEnv *env, jclass thisObj, jint x) {

    unsigned int newX = (unsigned int) x;

    return thomas_mueller_hash(newX);
}

JNIEXPORT jint JNICALL Java_com_github_metacachespark_HashFunctions_kmer2uint32 (JNIEnv *env, jclass thisObj, jstring input) {

    const char *newInput = (*env)->GetStringUTFChars(env, input, 0);

    return kmer2uint32C(newInput);

}