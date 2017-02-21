#include "com_github_jmabuin_metacachespark_HashFunctions.h"
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

inline unsigned int kmer2uint32C_IO(const char *input, unsigned int *kmer, int len, unsigned short *ambig) {


    int i = 0;
    //unsigned int kmer = 0x00000000u;
    unsigned int kmerMsk = 0xFFFFFFFFu;
    unsigned int ambigMsk = 0xFFFFFFFFu;

    kmerMsk >>= (sizeof(kmerMsk) * 8) - (len * 2);
    ambigMsk >>= (sizeof(ambigMsk) * 8) - len;

    *ambig = 0;

    for(i=0; i< len;i++) {

        *kmer <<= 2;
        *ambig <<= 1;

        switch(input[i]) {
            case 'A': case 'a': break;
            case 'C': case 'c': *kmer |= 1; break;
            case 'G': case 'g': *kmer |= 2; break;
            case 'T': case 't': *kmer |= 3; break;
            default: *ambig |= 1; break;
        }

    }
//fprintf(stderr,"[JMAbuin] Kmer. Done %s %u %s\n",input,kmer, __func__);
    return *kmer;


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

    int i = 0;
    int j = 0;
    int k = 0;

    int K = kmerSize;
    int J = 0;
    int len = kmerSize;

    //char kmerStr[kmerSize+1];
    //kmerStr[kmerSize] = '\0';

    unsigned int kmer32 = 0x00000000u;
    unsigned int hashValue;
    const char *windowTMP = window;

    unsigned int kmerMsk = 0xFFFFFFFFu;
    unsigned int ambigMsk = 0xFFFFFFFFu;
    unsigned short ambig;



    kmerMsk >>= (sizeof(kmerMsk) * 8) - (len * 2);
    ambigMsk >>= (sizeof(ambigMsk) * 8) - len;

    ambig = 0x0000u;
    //char kmersStr[strlen(window) - kmerSize][kmerSize+1];
    //unsigned int kmersU[strlen(window) - kmerSize];

    // Initialize array
    for (i = 0; i< sketchSize; i++) {

        sketchValues[i] = 0xFFFFFFFF;
    }

    // We compute the k-mers
    int tmpLen = kmerSize;

    //fprintf(stderr,"[JMAbuin] starting sketch fuction for window: %s\n", window);

    //for (i = 0, J = 0; (i < (strlen(window) - kmerSize)) && (J < K); i++, J++) {
    for (i = 0; i < (strlen(window) - kmerSize) && K>0; i++) {
        //while(K>0) {
            // Get the corresponding K-mer
            //strncpy(kmerStr,window+i, kmerSize);

            // Get canonical form
            //kmer2uint32C_IO(windowTMP, &kmer32, tmpLen);

            kmer32 <<= 2;
            ambig <<= 1;

            switch(windowTMP[i]) {
                case 'A': case 'a': break;
                case 'C': case 'c': kmer32 |= 1; break;
                case 'G': case 'g': kmer32 |= 2; break;
                case 'T': case 't': kmer32 |= 3; break;
                default: ambig |= 1; break;
            }

            //fprintf(stderr,"[JMAbuin] parsing new. kmer is %u - %d. ambig is %u %d and K is %d\n", kmer32, kmer32, ambig, ambig, K);

            --K;
            //make sure we load k letters at the beginning
            if((K == 0) && (!ambig)) {
                kmer32  &= kmerMsk;   //stamp out 2*k lower bits
                ambig &= ambigMsk;  //stamp out k lower bits

                //do something with the kmer (and the ambiguous letters flag)
                //consume(kmer, ambig);

                // Apply hash to current kmer
                //fprintf(stderr,"[JMAbuin] received value: %u %d\n", kmer32, kmer32);
                hashValue = thomas_mueller_hash(make_canonical32C(kmer32,16));//HashFunctions.make_canonical(this.hash_(kmer32), MCSConfiguration.kmerSize);
                //fprintf(stderr,"[JMAbuin] resulting value: %u %d\n", hashValue, hashValue);

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

                windowTMP+=tmpLen;
                tmpLen = 1;

                ++K; //we want only one letter next time
            }
        //}

    }

    return sketchValues;

}


inline unsigned int *seq2feat(const char *sequence, int windowSize, int sketchSize, int kmerSize) {

    unsigned int *sketchValues = (unsigned int *)malloc(sizeof(unsigned int) * sketchSize);

    int i = 0;
    int j = 0;
    int k = 0;
    int currentWindow = 0;

    int K = kmerSize ;
    int J = 0;
    int len = kmerSize;

    // (strlen(sequenceInput) / (windowSize-sketchSize))*sketchSize
    int numberOfWindows = strlen(sequence) / (windowSize - 16);

    unsigned int *features = (unsigned int *)malloc(sizeof(unsigned int) * numberOfWindows * sketchSize);

    unsigned int kmer32 = 0x00000000u;
    unsigned int hashValue;
    //const char *windowStart = sequence;
    //const char *windowEnd = windowStart + windowSize;

    unsigned int kmerMsk = 0xFFFFFFFFu;
    unsigned int ambigMsk = 0xFFFFFFFFu;
    unsigned short ambig;



    kmerMsk >>= (sizeof(kmerMsk) * 8) - (len * 2);
    ambigMsk >>= (sizeof(ambigMsk) * 8) - len;

    ambig = 0x0000u;


    // Initialize array


    // We compute the k-mers
    //int tmpLen = kmerSize;

    const char *windowTMP = sequence;
    //char *sequenceEnd = sequence + strlen(sequence); // +1??


    while(currentWindow < numberOfWindows){
        if(currentWindow == 0) {
            windowTMP = sequence;
        }
        else {
            windowTMP = sequence + (windowSize * currentWindow) - 16;
        }

        //fprintf(stderr,"[JMAbuin] Window: %d\n", currentWindow);

        kmer32 = 0x00000000u;
        kmerMsk = 0xFFFFFFFFu;
        ambigMsk = 0xFFFFFFFFu;

        kmerMsk >>= (sizeof(kmerMsk) * 8) - (len * 2);
        ambigMsk >>= (sizeof(ambigMsk) * 8) - len;

        ambig = 0x0000u;


        for (i = 0; i< sketchSize; i++) {

            sketchValues[i] = 0xFFFFFFFF;
        }

        K = kmerSize ;

        // Iterate over current window
        // i: iterate over window chars
        // J: first 16 chars, then one per iteration
        for (i = 0, J = 0; (i < (windowSize - kmerSize)) && (J < K); i++, J++) {

            kmer32 <<= 2;
            ambig <<= 1;

            switch(windowTMP[i]) {
                case 'A': case 'a': break;
                case 'C': case 'c': kmer32 |= 1; break;
                case 'G': case 'g': kmer32 |= 2; break;
                case 'T': case 't': kmer32 |= 3; break;
                default: ambig |= 1; break;
            }

            --K;
            //make sure we load k letters at the beginning

            if((K == 0) && (!ambig)) {
                kmer32  &= kmerMsk;   //stamp out 2*k lower bits
                ambig &= ambigMsk;  //stamp out k lower bits

                //do something with the kmer (and the ambiguous letters flag)
                //consume(kmer, ambig);

                // Apply hash to current kmer
                hashValue = thomas_mueller_hash(make_canonical32C(kmer32,16));//HashFunctions.make_canonical(this.hash_(kmer32), MCSConfiguration.kmerSize);

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

                ++K; //we want only one letter next time
            }

        }
        //fprintf(stderr,"[JMAbuin] Before memcpy window: %d\n", currentWindow);

        memcpy(&features[currentWindow*sketchSize],sketchValues,sketchSize * sizeof(unsigned int));
        //fprintf(stderr,"[JMAbuin] After memcpy window: %d\n", currentWindow);
        currentWindow++;
    }

    return features;
}

JNIEXPORT jlong JNICALL Java_com_github_jmabuin_metacachespark_HashFunctions_make_1reverse_1complement64 (JNIEnv *env, jclass thisObj, jlong s, jint k) {

    unsigned long newS = (unsigned long) s;
    unsigned char newK = (unsigned char) k;

    return make_reverse_complement64C(newS, newK);


}

JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_HashFunctions_make_1reverse_1complement32 (JNIEnv *env, jclass thisObj, jint s, jint k) {

    unsigned int newS = (unsigned int) s;
    unsigned char newK = (unsigned char) k;

    return make_reverse_complement32C(newS, newK);

}

JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_HashFunctions_thomas_1mueller_1hash32  (JNIEnv *env, jclass thisObj, jint x) {

    unsigned int newX = (unsigned int) x;

    return thomas_mueller_hash(newX);
}

JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_HashFunctions_kmer2uint32 (JNIEnv *env, jclass thisObj, jstring input) {

    const char *newInput = (*env)->GetStringUTFChars(env, input, 0);

    return kmer2uint32C(newInput);

}

JNIEXPORT jintArray JNICALL Java_com_github_jmabuin_metacachespark_HashFunctions_window2sketch32 (JNIEnv *env, jclass thisObj, jstring window, jint sketchSize, jint kmerSize) {

    const char *windowInput = (*env)->GetStringUTFChars(env, window, 0);
    jintArray iarr = (*env)->NewIntArray(env, sketchSize);

    //fprintf(stderr,"[JMAbuin] Antes de w2s %s\n", __func__);


    int *returnValues = (int *)window2sketch(windowInput, sketchSize, kmerSize);

    /*fprintf(stderr,"[JMAbuin] Despois de w2s %s\n",__func__);
    int i = 0;

    for(i = 0; i< sketchSize; i++) {
        fprintf(stderr,"[JMAbuin] Despois de w2s %u %d, %s\n",returnValues[i],returnValues[i],__func__);
    }*/

    (*env)->SetIntArrayRegion(env, iarr, 0, sketchSize, returnValues);
    return iarr;

}


JNIEXPORT jintArray JNICALL Java_com_github_jmabuin_metacachespark_HashFunctions_sequence2features (JNIEnv *env, jclass thisObj, jstring sequence, jint windowSize, jint sketchSize, jint kmerSize) {
    const char *sequenceInput = (*env)->GetStringUTFChars(env, sequence, 0);
    jintArray iarr = (*env)->NewIntArray(env, (strlen(sequenceInput) / (windowSize-sketchSize))*sketchSize);

    int *returnValues = (int *)seq2feat(sequenceInput, windowSize, sketchSize, kmerSize);
    //fprintf(stderr,"[JMAbuin] Before prepairing data to return\n");

    (*env)->SetIntArrayRegion(env, iarr, 0, (strlen(sequenceInput) / (windowSize-sketchSize))*sketchSize, returnValues);

    //fprintf(stderr,"[JMAbuin] After prepairing data to return\n");
    return iarr;
}