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
//fprintf(stderr,"[JMAbuin] Kmer. Done %s %u %s\n",input,kmer, __func__);
    return kmer;


}

inline unsigned int make_canonical32C(unsigned int s, unsigned char k) {
	unsigned int revcom = make_reverse_complement32C(s, k);
	//fprintf(stderr,"[JMAbuin] Canonical. Done %u %u %s\n",s,revcom, __func__);

	return s < revcom ? s : revcom;
}

inline unsigned long make_canonical64C(unsigned long s, unsigned char k) {
	unsigned long revcom = make_reverse_complement64C(s, k);
	return s < revcom ? s : revcom;
}

inline unsigned int *window2sketch(const char *window, int sketchSize, int kmerSize) {

    unsigned int *sketchValues = (unsigned int *)malloc(sizeof(unsigned int) * sketchSize);

    int i = 0, j, k;
    char kmerStr[kmerSize+1];
    kmerStr[kmerSize] = '\0';

    unsigned int kmer32;
    unsigned int hashValue;

    //char kmersStr[strlen(window) - kmerSize][kmerSize+1];
    //unsigned int kmersU[strlen(window) - kmerSize];

    // Initialize array
    for (i = 0; i< sketchSize; i++) {

        sketchValues[i] = 0xFFFFFFFF;
    }

    // We compute the k-mers
    for (i = 0; i < (strlen(window) - kmerSize); i++) {

        // Get the corresponding K-mer
        strncpy(kmerStr,window+i, kmerSize);

        // Get canonical form
    	kmer32 = make_canonical32C(kmer2uint32C(kmerStr), 16);

    	// Apply hash to current kmer
    	hashValue = thomas_mueller_hash(kmer32);//HashFunctions.make_canonical(this.hash_(kmer32), MCSConfiguration.kmerSize);

        // Insert into array if needed
        if(hashValue < sketchValues[sketchSize-1]) {

            for(j = 0 ; j < sketchSize ; j++){
                if(hashValue < sketchValues[j])
                    break;
            }

            // We dont need this IF. It is checked before
            //if(j < sketchSize) {

                for(k = sketchSize - 2; k>=j; k--){
                    sketchValues[k+1] = sketchValues[k];
                }

                sketchValues[j] = hashValue;
            //}

 	    }
    }

/*
    for (i = 0; i < (strlen(window) - kmerSize); i++) {

            // Get the corresponding K-mer
            strncpy(kmersStr[i],window+i, kmerSize);

    }

    for(i = 0; i < (strlen(window) - kmerSize); i++) {
        kmersU[i] = thomas_mueller_hash(make_canonical32C(kmer2uint32C(kmersStr[i]), 16));
    }

    for(i = 0; i < (strlen(window) - kmerSize); i++) {
            if(kmersU[i] < sketchValues[sketchSize-1]) {

                        for(j = 0 ; j < sketchSize ; j++){
                            if(kmersU[i] < sketchValues[j])
                                break;
                        }

                        // We dont need this IF. It is checked before
                        //if(j < sketchSize) {

                            for(k = sketchSize - 2; k>=j; k--){
                                sketchValues[k+1] = sketchValues[k];
                            }

                            sketchValues[j] = kmersU[i];
                        //}

             	    }

    }
*/
    return sketchValues;

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

JNIEXPORT jintArray JNICALL Java_com_github_metacachespark_HashFunctions_window2sketch32 (JNIEnv *env, jclass thisObj, jstring window, jint sketchSize, jint kmerSize) {

    const char *windowInput = (*env)->GetStringUTFChars(env, window, 0);
    jintArray iarr = (*env)->NewIntArray(env, sketchSize);

    //fprintf(stderr,"[JMAbuin] Antes de w2s %s\n", __func__);
    int *returnValues = (int *)window2sketch(windowInput, sketchSize, kmerSize);
    //fprintf(stderr,"[JMAbuin] Despois de w2s %s\n",__func__);
    (*env)->SetIntArrayRegion(env, iarr, 0, sketchSize, returnValues);
    return iarr;

}