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
#include <set>

//HashMultiMap *map;
mc::hash_multimap<unsigned, location> *map = nullptr;
std::set<unsigned> marked_for_deletion;
int max_locations;


JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_init (JNIEnv *env, jobject jobj, jint max_size) {

    //map = new HashMultiMap();
    if (map != nullptr) {
        map->clear();
    }
    else {
        map = new mc::hash_multimap<unsigned, location>();
    }

    max_locations = (int) max_size;
    return 1;
}


JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_add (JNIEnv *env, jobject jobj, jint key, jint v1, jint v2) {

    location newLocation = location((unsigned)v1, (unsigned)v2);

    unsigned int unsigned_key = (unsigned)key;

    //map->insert(unsigned_key, newLocation);
    auto it = map->insert(unsigned_key, newLocation);
    if(it->size() > max_locations) {
        map->shrink(it, max_locations);
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
    write_binary(ofs, *map);
    //write_natural(ofs, *map);

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

JNIEXPORT jintArray JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_keys (JNIEnv *env, jobject jobj) {

    jintArray iarr = env->NewIntArray(map->non_empty_bucket_count());

    int *keys = (int *)malloc(sizeof(int) * map->non_empty_bucket_count());

    //std::unordered_map<unsigned, std::vector<location>>::iterator it = map->begin();

    unsigned i = 0;
    for(const auto& b : map->buckets_) {
                if(!b.empty()) {
                    keys[i] = b.key();
                    ++i;
                }
            }


    env->SetIntArrayRegion(iarr, 0, map->non_empty_bucket_count(), keys);

    free(keys);

    return iarr;

}

JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_post_1process (JNIEnv *env, jobject jobj, jboolean over_populated, jboolean ambiguous) {
    if(over_populated) {

            auto maxlpf = max_locations - 1;
            if(maxlpf > 0) { //always keep buckets with size 1

                //note that features are not really removed, because the hashmap
                //does not support erasing keys; instead all values belonging to
                //the key are cleared and the key is kept without values
                int rem = 0;
                for(auto i = map->begin(), e = map->end(); i != e; ++i) {
                    if(i->size() > maxlpf) {
                        map->clear(i);
                        //map->clear(i);
                        ++rem;
                    }
                }
                return rem;
        }
    }

    return 1;

}

JNIEXPORT void JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_clear (JNIEnv *env, jobject jobj) {

    for(auto bucket = map->begin(); bucket!=map->end(); ++bucket) {

        map->clear(bucket);


    }

    map->clear();

    //delete map;

  }

JNIEXPORT void JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_clear_1key (JNIEnv *env, jobject jobj, jint javaKey) {

    unsigned int key = (unsigned int) javaKey;

    auto sit = map->find(key);

    if(sit != map->end()) {

        map->clear(sit);

    }

}

/*
 * Class:     com_github_jmabuin_metacachespark_database_HashMultiMapNative
 * Method:    get_size_of_key
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_get_1size_1of_1key (JNIEnv *env, jobject obj, jint javaKey) {

    unsigned int key = (unsigned int) javaKey;

    auto sit = map->find(key);

    if(sit != map->end()) {

        return sit->size();

    }

    return 0;

}