/**
 * Copyright 2019 José Manuel Abuín Mosquera <josemanuel.abuin@usc.es>
 *
 * <p>This file is part of MetaCacheSpark.
 *
 * <p>MetaCacheSpark is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * <p>MetaCacheSpark is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * <p>You should have received a copy of the GNU General Public License along with MetaCacheSpark. If not,
 * see <http://www.gnu.org/licenses/>.
 */

#include "com_github_jmabuin_metacachespark_database_HashMultiMapNative.h"
#include "hash_multimap.h"

#include <fstream>

//HashMultiMap *map;
mc::hash_multimap<unsigned, location> *map;

JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_init (JNIEnv *env, jobject jobj) {

    //map = new HashMultiMap();
    map = new mc::hash_multimap<unsigned, location>();

    return 1;
}

JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_add (JNIEnv *env, jobject jobj, jint key, jint v1, jint v2) {

    location newLocation = location((unsigned)v1, (unsigned)v2);

    unsigned int unsigned_key = (unsigned)key;

    //map->insert(unsigned_key, newLocation);
    auto it = map->insert(unsigned_key, newLocation);
    if(it->size() > 254) {
        map->shrink(it, 254);
    }

    //int numItems = 7;

    return map->bucket_count();

}

JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_size (JNIEnv *env, jobject jobj) {

    //return map->getNumKeys();
    return map->key_count();
}

JNIEXPORT void JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_write (JNIEnv *env, jobject jobj, jstring fileName) {

    const char *nativeString = env->GetStringUTFChars(fileName, 0);

    std::string newFileName(nativeString);

    std::ofstream ofs;

    ofs.open(newFileName);

    if (!ofs.good()) {
        std::cerr << "Can't open file" << newFileName << std::endl;
        exit(1);
    }

    //map->write_binary_to_file(newFileName);
    //write_binary(ofs, *map);
    write_natural(ofs, *map);

    ofs.close();
    env->ReleaseStringUTFChars(fileName, nativeString);
  }

JNIEXPORT void JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_read (JNIEnv *env, jobject jobj, jstring fileName) {
    const char *nativeString = env->GetStringUTFChars(fileName, 0);

    std::string newFileName(nativeString);
    //map->read_binary_from_file(newFileName);
    std::ifstream ifs;

        ifs.open(newFileName);

        if (!ifs.good()) {
            std::cerr << "Can't open file" << newFileName << std::endl;
            exit(1);
        }

        //map->write_binary_to_file(newFileName);
        read_binary(ifs, *map);

        ifs.close();



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
        //std::cerr << "Number of items for key: " << key << " is " << sit->size() << std::endl;
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
    //else {
    //    std::cerr << "No bucket for key: " << key << std::endl;
    //}

    return iarr;

}