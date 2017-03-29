#include "com_github_jmabuin_metacachespark_database_HashMultiMapNative.h"
#include "hashmultimap.h"


HashMultiMap *map;

JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_init (JNIEnv *env, jobject jobj) {

    map = new HashMultiMap();

    return 1;
}

JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_add (JNIEnv *env, jobject jobj, jint key, jint v1, jint v2) {

    location newLocation = location(v1, v2);
    map->insert(key, newLocation);

    //int numItems = 7;

    return map->bucket_count();


}

JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_size (JNIEnv *env, jobject jobj) {

    //map->show();
    return map->getNumKeys();
}

JNIEXPORT void JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_write (JNIEnv *env, jobject jobj, jstring fileName) {

    const char *nativeString = env->GetStringUTFChars(fileName, 0);

    std::string newFileName(nativeString);
    map->write_binary_to_file(newFileName);

    env->ReleaseStringUTFChars(fileName, nativeString);
  }

JNIEXPORT void JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_read (JNIEnv *env, jobject jobj, jstring fileName) {
    const char *nativeString = env->GetStringUTFChars(fileName, 0);

    std::string newFileName(nativeString);
    map->read_binary_from_file(newFileName);

    env->ReleaseStringUTFChars(fileName, nativeString);
}

JNIEXPORT jintArray JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_get (JNIEnv *env, jobject jobj, jint javaKey){

    unsigned int key = (unsigned int) javaKey;

    auto sit = map->find(key);

    jintArray iarr = nullptr;

    int *foundValues;

    if(sit != map->end()) {

        iarr = env->NewIntArray(sit->size()*2);
        foundValues = (int *)malloc(sizeof(int) * sit->size() * 2);

        int i = 0;

        for(const auto& pos : *sit) {
            //consume(pos);
            foundValues[i] = pos.tgt;
            foundValues[i+1] = pos.win;
            i+=2;
        }

        env->SetIntArrayRegion(iarr, 0, sit->size() * 2, foundValues);
        free(foundValues);
    }

    return iarr;

}