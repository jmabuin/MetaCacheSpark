#include "com_github_jmabuin_metacachespark_LocationsSortedList.h"

#include "../location.h"

#include <vector>
#include <algorithm>

std::vector<location> *my_vector;
JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_LocationsSortedList_init (JNIEnv *env, jobject jobj) {

    my_vector = new std::vector<location>();

    return 1;
}

/*
 * Class:     com_github_jmabuin_metacachespark_LocationsSortedList
 * Method:    add
 * Signature: (II)I
 */
JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_LocationsSortedList_add  (JNIEnv *env, jobject jobj, jint tgt, jint win) {

    location newLocation = location((unsigned)tgt, (unsigned)win);


    my_vector->insert(std::upper_bound( my_vector->begin(), my_vector->end(), newLocation ), newLocation );
    return my_vector->size();
}

/*
 * Class:     com_github_jmabuin_metacachespark_LocationsSortedList
 * Method:    get
 * Signature: (I)[I
 */
JNIEXPORT jintArray JNICALL Java_com_github_jmabuin_metacachespark_LocationsSortedList_get (JNIEnv *env, jobject jobj, jint key) {

    location current = my_vector->at((int) key);

    int *foundValues = (int *)malloc(sizeof(int) * 2);

    foundValues[0] = current.tgt;
    foundValues[1] = current.win;

    jintArray iarr = env->NewIntArray(2);

    env->SetIntArrayRegion(iarr, 0, 2, foundValues);

    free(foundValues);

    return iarr;
}

/*
 * Class:     com_github_jmabuin_metacachespark_LocationsSortedList
 * Method:    size
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_LocationsSortedList_size (JNIEnv *env, jobject jobj) {


    return my_vector->size();
}