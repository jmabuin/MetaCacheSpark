#include "com_github_metacachespark_HashFunctions.h"

unsigned long make_reverse_complement64C(unsigned long s, unsigned char k) {
    s = ((s >> 2)  & 0x3333333333333333ul) | ((s & 0x3333333333333333ul) << 2);
    s = ((s >> 4)  & 0x0F0F0F0F0F0F0F0Ful) | ((s & 0x0F0F0F0F0F0F0F0Ful) << 4);
    s = ((s >> 8)  & 0x00FF00FF00FF00FFul) | ((s & 0x00FF00FF00FF00FFul) << 8);
    s = ((s >> 16) & 0x0000FFFF0000FFFFul) | ((s & 0x0000FFFF0000FFFFul) << 16);
    s = ((s >> 32) & 0x00000000FFFFFFFFul) | ((s & 0x00000000FFFFFFFFul) << 32);
    return ((unsigned long)(-1) - s) >> (8 * sizeof(s) - (k << 1));
}

//-------------------------------------------------------------------
unsigned int make_reverse_complement32C(unsigned int s, unsigned char k) {
    s = ((s >> 2)  & 0x33333333u) | ((s & 0x33333333u) << 2);
    s = ((s >> 4)  & 0x0F0F0F0Fu) | ((s & 0x0F0F0F0Fu) << 4);
    s = ((s >> 8)  & 0x00FF00FFu) | ((s & 0x00FF00FFu) << 8);
    s = ((s >> 16) & 0x0000FFFFu) | ((s & 0x0000FFFFu) << 16);
    return ((unsigned int)(-1) - s) >> (8 * sizeof(s) - (k << 1));
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


