#include "com_github_jmabuin_metacachespark_database_MapNative.h"
#include "../location.h"

#include <map>

std::map<location, unsigned int> *myMap;
std::map<location, unsigned int>::iterator myIterator;
std::map<location, unsigned int>::iterator FSTIterator;
std::map<location, unsigned int>::iterator LSTIterator;

JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_database_MapNative_init (JNIEnv *env, jobject jobj) {

    myMap = new std::map<location, unsigned int>();
    myIterator = myMap->begin();
    FSTIterator = myMap->begin();
    LSTIterator = myMap->begin();
    return 1;

}

JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_database_MapNative_add (JNIEnv *env, jobject jobj,
    jint tgt, jint win, jint data) {

    location newLocation;

    newLocation.tgt = (unsigned int) tgt;
    newLocation.win = (unsigned int) win;

    std::map<location,unsigned int>::iterator it;

    it = myMap->find(newLocation);

    if(it != myMap->end()) {

        myMap->at(newLocation) = myMap->at(newLocation) + 1;

    }
    else {

        //if(myMap->size() < 256) {
            myMap->insert(std::pair<location, unsigned int>(newLocation, 1));
        //}
    }

    return myMap->size();

}

JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_database_MapNative_get
  (JNIEnv *env, jobject jobj, jint tgt, jint win) {

    location newLocation;

    newLocation.tgt = (unsigned int) tgt;
    newLocation.win = (unsigned int) win;


    return (int) myMap->at(newLocation);

}


JNIEXPORT jintArray JNICALL Java_com_github_jmabuin_metacachespark_database_MapNative_get_1by_1pos
  (JNIEnv *env, jobject jobj, jint position) {

    /*std::pair<location, unsigned int> values = myMap->[position];

    unsigned int tgt = values.first.tgt;
    unsigned int win = values.first.win;
    unsigned int value = values.second;

    jintArray iarr = env->NewIntArray(3);

    int *foundValues = (int *)malloc(sizeof(int) * 3);

    foundValues[0] = (int) tgt;
    foundValues[1] = (int) win;
    foundValues[2] = (int) value;

    env->SetIntArrayRegion(iarr, 0, 3, foundValues);
    free(foundValues);


    return iarr;
*/
    jintArray iarr = env->NewIntArray(3);
    return iarr;
  }


JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_database_MapNative_size
    (JNIEnv *env, jobject jobj) {

    return myMap->size();

}

JNIEXPORT void JNICALL Java_com_github_jmabuin_metacachespark_database_MapNative_clear
  (JNIEnv *env, jobject jobj) {

  myMap->clear();

}

JNIEXPORT void JNICALL Java_com_github_jmabuin_metacachespark_database_MapNative_resetIterator
  (JNIEnv *env, jobject jobj) {

    myIterator = myMap->begin();
}

JNIEXPORT jboolean JNICALL Java_com_github_jmabuin_metacachespark_database_MapNative_isEmpty
  (JNIEnv *env, jobject jobj) {

    if(myMap->empty()) {

        return JNI_TRUE;
    }
    else {

        return JNI_FALSE;
    }

}

JNIEXPORT jintArray JNICALL Java_com_github_jmabuin_metacachespark_database_MapNative_get_1current
  (JNIEnv *env, jobject jobj) {

    location currentLocation = myIterator->first;

    unsigned int tgt = currentLocation.tgt;
    unsigned int win = currentLocation.win;

    unsigned int value = myIterator->second;

    jintArray iarr = env->NewIntArray(3);

    int *foundValues = (int *)malloc(sizeof(int) * 3);

    foundValues[0] = (int) tgt;
    foundValues[1] = (int) win;
    foundValues[2] = (int) value;

    env->SetIntArrayRegion(iarr, 0, 3, foundValues);
    free(foundValues);


    return iarr;
}

JNIEXPORT void JNICALL Java_com_github_jmabuin_metacachespark_database_MapNative_next
  (JNIEnv *env, jobject jobj) {

    if(myIterator != myMap->end()) {
        myIterator++;
    }
    else {
        myIterator = myMap->end();
    }
}

JNIEXPORT jboolean JNICALL Java_com_github_jmabuin_metacachespark_database_MapNative_isEnd
  (JNIEnv *env, jobject jobj) {

    if(myIterator != myMap->end()) {
            return JNI_FALSE;
    }
    else {
        return JNI_TRUE;
    }
}

JNIEXPORT void JNICALL Java_com_github_jmabuin_metacachespark_database_MapNative_resetFSTIterator
  (JNIEnv *env, jobject jobj) {

    FSTIterator = myMap->begin();
}

JNIEXPORT void JNICALL Java_com_github_jmabuin_metacachespark_database_MapNative_resetLSTIterator
  (JNIEnv *env, jobject jobj){

    LSTIterator = myMap->begin();
}

JNIEXPORT jintArray JNICALL Java_com_github_jmabuin_metacachespark_database_MapNative_get_1currentFST
  (JNIEnv *env, jobject jobj) {

    location currentLocation = FSTIterator->first;

    unsigned int tgt = currentLocation.tgt;
    unsigned int win = currentLocation.win;

    unsigned int value = FSTIterator->second;

    jintArray iarr = env->NewIntArray(3);

    int *foundValues = (int *)malloc(sizeof(int) * 3);

    foundValues[0] = (int) tgt;
    foundValues[1] = (int) win;
    foundValues[2] = (int) value;

    env->SetIntArrayRegion(iarr, 0, 3, foundValues);
    free(foundValues);


    return iarr;

  }

JNIEXPORT jintArray JNICALL Java_com_github_jmabuin_metacachespark_database_MapNative_get_1currentLST
  (JNIEnv *env, jobject jobj) {
    location currentLocation = LSTIterator->first;

    unsigned int tgt = currentLocation.tgt;
    unsigned int win = currentLocation.win;

    unsigned int value = LSTIterator->second;

    jintArray iarr = env->NewIntArray(3);

    int *foundValues = (int *)malloc(sizeof(int) * 3);

    foundValues[0] = (int) tgt;
    foundValues[1] = (int) win;
    foundValues[2] = (int) value;

    env->SetIntArrayRegion(iarr, 0, 3, foundValues);
    free(foundValues);


    return iarr;
  }

JNIEXPORT void JNICALL Java_com_github_jmabuin_metacachespark_database_MapNative_nextFST
  (JNIEnv *env, jobject jobj) {

    if(FSTIterator != myMap->end()) {
        FSTIterator++;
    }
    else {
        FSTIterator = myMap->end();
    }

  }

JNIEXPORT void JNICALL Java_com_github_jmabuin_metacachespark_database_MapNative_nextLST
  (JNIEnv *env, jobject jobj) {
    if(LSTIterator != myMap->end()) {
        LSTIterator++;
    }
    else {
        LSTIterator = myMap->end();
    }
  }

JNIEXPORT jboolean JNICALL Java_com_github_jmabuin_metacachespark_database_MapNative_isEndFST
  (JNIEnv *env, jobject jobj) {
    if(FSTIterator != myMap->end()) {
            return JNI_FALSE;
    }
    else {
        return JNI_TRUE;
    }
  }

JNIEXPORT jboolean JNICALL Java_com_github_jmabuin_metacachespark_database_MapNative_isEndLST
  (JNIEnv *env, jobject jobj) {
    if(LSTIterator != myMap->end()) {
            return JNI_FALSE;
    }
    else {
        return JNI_TRUE;
    }
  }

JNIEXPORT void JNICALL Java_com_github_jmabuin_metacachespark_database_MapNative_setFST2LST
  (JNIEnv *env, jobject jobj) {

    FSTIterator = LSTIterator;
  }