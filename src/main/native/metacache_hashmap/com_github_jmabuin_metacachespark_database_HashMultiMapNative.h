/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class com_github_jmabuin_metacachespark_database_HashMultiMapNative */

#ifndef _Included_com_github_jmabuin_metacachespark_database_HashMultiMapNative
#define _Included_com_github_jmabuin_metacachespark_database_HashMultiMapNative
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_github_jmabuin_metacachespark_database_HashMultiMapNative
 * Method:    init
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_init
  (JNIEnv *, jobject, jint);

/*
 * Class:     com_github_jmabuin_metacachespark_database_HashMultiMapNative
 * Method:    add
 * Signature: (III)I
 */
JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_add
  (JNIEnv *, jobject, jint, jint, jint);

/*
 * Class:     com_github_jmabuin_metacachespark_database_HashMultiMapNative
 * Method:    get
 * Signature: (I)[I
 */
JNIEXPORT jintArray JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_get
  (JNIEnv *, jobject, jint);

/*
 * Class:     com_github_jmabuin_metacachespark_database_HashMultiMapNative
 * Method:    keys
 * Signature: ()[I
 */
JNIEXPORT jintArray JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_keys
  (JNIEnv *, jobject);

/*
 * Class:     com_github_jmabuin_metacachespark_database_HashMultiMapNative
 * Method:    size
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_size
  (JNIEnv *, jobject);

/*
 * Class:     com_github_jmabuin_metacachespark_database_HashMultiMapNative
 * Method:    write
 * Signature: (Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_write
  (JNIEnv *, jobject, jstring);

/*
 * Class:     com_github_jmabuin_metacachespark_database_HashMultiMapNative
 * Method:    read
 * Signature: (Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_read
  (JNIEnv *, jobject, jstring);

/*
 * Class:     com_github_jmabuin_metacachespark_database_HashMultiMapNative
 * Method:    post_process
 * Signature: (ZZ)I
 */
JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_post_1process
  (JNIEnv *, jobject, jboolean, jboolean);

/*
 * Class:     com_github_jmabuin_metacachespark_database_HashMultiMapNative
 * Method:    clear
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_clear
  (JNIEnv *, jobject);

/*
 * Class:     com_github_jmabuin_metacachespark_database_HashMultiMapNative
 * Method:    clear_key
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_clear_1key
  (JNIEnv *, jobject, jint);

/*
 * Class:     com_github_jmabuin_metacachespark_database_HashMultiMapNative
 * Method:    get_size_of_key
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_get_1size_1of_1key
  (JNIEnv *, jobject, jint);

/*
 * Class:     com_github_jmabuin_metacachespark_database_HashMultiMapNative
 * Method:    add_key_to_delete
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_add_1key_1to_1delete
  (JNIEnv *, jobject, jint);

#ifdef __cplusplus
}
#endif
#endif
