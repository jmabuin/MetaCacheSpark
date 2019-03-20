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

#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <set>

#include "io_serialize.h"
#include "../location.h"

//HashMultiMap *map;
std::unordered_map<unsigned, std::vector<location>> *map = nullptr;
std::set<unsigned> marked_for_deletion;
int max_locations;

size_t add_to_map(unsigned key, location loc) {

    auto sid = map->find(key);

        if (sid != map->end()) {
            //if(std::find(v.begin(), v.end(), x) != v.end()) {
            if ((sid->second.size() < max_locations) && (std::find(sid->second.begin(), sid->second.end(), loc) == sid->second.end())) {
                sid->second.insert(std::upper_bound( sid->second.begin(), sid->second.end(), loc ), loc);
                //sid->second.push_back(loc);
            }
            else if ((sid->second.size() >= max_locations) && (std::find(sid->second.begin(), sid->second.end(), loc) == sid->second.end())) {

                //marked_for_deletion.insert(key);
                std::vector<location>::iterator pos = std::upper_bound( sid->second.begin(), sid->second.end(), loc );

                if (pos != sid->second.end()) {

                    sid->second.insert(pos, loc);
                    sid->second.resize(max_locations);
                }


            }

        }
        else {

            std::vector<location> new_vector;
            new_vector.push_back(loc);
            //new_vector.push_back(value2);

            std::pair<unsigned, std::vector<location>> new_item(key, new_vector);
            map->insert(new_item);

        }

        return map->size();
}

void serialize(std::ostream& os) {

            mc::write_binary(os, map->size());
            //write_binary(os, len_t(value_count()));

            std::vector<unsigned> target_id_buf;
            std::vector<unsigned> window_id_buf;

            //for(const auto& bucket : buckets_) {
            for(auto bucket = map->begin(); bucket!=map->end(); ++bucket) {
                //if(!bucket.empty()) {
                    mc::write_binary(os, bucket->first);
                    mc::write_binary(os, bucket->second.size());

                    target_id_buf.clear();
                    target_id_buf.reserve(bucket->second.size());
                    window_id_buf.clear();
                    window_id_buf.reserve(bucket->second.size());

                    for(const auto& v : bucket->second) {
                        target_id_buf.emplace_back(v.tgt);
                        window_id_buf.emplace_back(v.win);
                    }

                    mc::write_binary(os, target_id_buf);
                    mc::write_binary(os, window_id_buf);
                //}
            }

}


void deserialize(std::istream& is) {

        size_t nkeys = 0;
        mc::read_binary(is, nkeys);
        //len_t nvalues = 0;
        //read_binary(is, nvalues);

        if(nkeys > 0) {

            std::vector<unsigned> target_id_buf;
            std::vector<unsigned> window_id_buf;

            for(size_t i = 0; i < nkeys; ++i) {
                unsigned key;
                size_t nvals = 0;
                mc::read_binary(is, key);
                mc::read_binary(is, nvals);

                if(nvals > 0) {
                    mc::read_binary(is, target_id_buf);
                    mc::read_binary(is, window_id_buf);



                    for(size_t i = 0; i < nvals; ++i) {
                        add_to_map(key, location{target_id_buf[i], window_id_buf[i]});
                    }
                }
            }

        }
    }



JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_init (JNIEnv *env, jobject jobj, jint max_size) {

    //map = new HashMultiMap();
    if (map != nullptr) {
        map->clear();
    }
    else {
        map = new std::unordered_map<unsigned, std::vector<location>>();
    }

    max_locations = (int) max_size;
    return 1;
}

JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_add (JNIEnv *env, jobject jobj, jint key, jint v1, jint v2) {

    unsigned int unsigned_key = (unsigned)key;

    //if (unsigned_key == 0) {

    //    return map->size();
    //}

    int value1 = (int) v1;
    int value2 = (int) v2;

    location newLocation = location((unsigned)v1, (unsigned)v2);

        //map->insert(unsigned_key, newLocation);
/*
    auto sid = map->find(unsigned_key);

    if (sid != map->end()) {
        //if(std::find(v.begin(), v.end(), x) != v.end()) {
        if ((sid->second.size() < 254) && (std::find(sid->second.begin(), sid->second.end(), newLocation) == sid->second.end())) {
        //if (std::find(sid->second.begin(), sid->second.end(), newLocation) == sid->second.end()) {
            //sid->second.push_back(newLocation);
            sid->second.insert(std::upper_bound( sid->second.begin(), sid->second.end(), newLocation ),
                        newLocation
                    );
        }
        else if ((sid->second.size() >= 254) && (std::find(sid->second.begin(), sid->second.end(), newLocation) == sid->second.end())) {

            marked_for_deletion.insert(unsigned_key);
            std::vector<location>::iterator pos = std::upper_bound( sid->second.begin(), sid->second.end(), newLocation );

            if (pos != sid->second.end()) {

                sid->second.insert(pos, newLocation);
                sid->second.resize(254);
            }


        }

    }
    else {

        std::vector<location> new_vector;
        new_vector.push_back(newLocation);
        //new_vector.push_back(value2);

        std::pair<unsigned, std::vector<location>> new_item(unsigned_key, new_vector);
        map->insert(new_item);

    }

    return map->size();
    */ return add_to_map(unsigned_key, newLocation);

}

JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_size (JNIEnv *env, jobject jobj) {

    //return map->getNumKeys();
    return map->size();
}

JNIEXPORT void JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_write (JNIEnv *env, jobject jobj, jstring fileName) {

    const char *nativeString = env->GetStringUTFChars(fileName, 0);

    std::string newFileName(nativeString);
/*
    std::ofstream ofs;

    ofs.open(newFileName);

    if (!ofs.good()) {
        std::cerr << "Can't open file" << newFileName << std::endl;
        exit(1);
    }

    std::unordered_map<unsigned, std::vector<location>>::iterator it;

    std::vector<location> current_vector;

    for(it = map->begin(); it != map->end(); ++it) {

        ofs << it->first << ":";
        current_vector = it->second;

        for (int i = 0; i< current_vector.size(); ++i ) {

            ofs << current_vector[i].tgt << ";" << current_vector[i].win << ";";
        }

        ofs << "\n";
    }

    ofs.close();
*/

    std::ofstream os{newFileName, std::ios::out | std::ios::binary};

    if(!os.good()) {
        std::cerr << "Can't open file" << newFileName << std::endl;
        exit(1);
    }

    serialize(os);

    os.close();

    env->ReleaseStringUTFChars(fileName, nativeString);
  }

JNIEXPORT void JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_read (JNIEnv *env, jobject jobj, jstring fileName) {
    const char *nativeString = env->GetStringUTFChars(fileName, 0);

    std::string newFileName(nativeString);
    //map->read_binary_from_file(newFileName);
    /*std::ifstream ifs;

    ifs.open(newFileName);

    if (!ifs.good()) {
        std::cerr << "Can't open file" << newFileName << std::endl;
        exit(1);
    }

    std::string line;
        std::string key_delimiter = ":";
        char values_delimiter = ';';
        unsigned key;

        std::string token;

        std::string key_string;
        std::string values_string;

        std::istringstream tokenStream;
        std::vector<int> new_vector;
        std::vector<location> locations_vector;
        //std::cout << "Starting to read lines..." << std::endl;

        while (getline(ifs, line)) {
            key_string = line.substr(0, line.find(key_delimiter));
            //std::cout << "Current key... " << key_string << std::endl;
            //key = std::stoul (key_string, nullptr, 0);
            key = std::stoul (key_string, nullptr, 0);
            //std::cout << "Now line" << std::endl;
            values_string = line.substr(line.find(key_delimiter)+1, line.length());

            tokenStream = std::istringstream (values_string);

            tokenStream.clear();
            values_string.clear();
            key_string.clear();
            new_vector.clear();
            locations_vector.clear();

            while (std::getline(tokenStream, token, values_delimiter)) {
                if (token!="\n") {
                    new_vector.push_back(std::stoi(token));
                }

            }

            for(int i = 0; i< new_vector.size(); i+=2) {
                locations_vector.push_back(location((unsigned)new_vector[i], (unsigned)new_vector[i+1]));

            }


            std::pair<unsigned,std::vector<location>> new_item (key, locations_vector);
            map->insert(new_item);

        }

    ifs.close();
*/

std::ifstream is{newFileName, std::ios::in | std::ios::binary};

        if(!is.good()) {
            std::cerr << "Can't open file" << newFileName << std::endl;
                    exit(1);
        }

        deserialize(is);


    env->ReleaseStringUTFChars(fileName, nativeString);
}

JNIEXPORT jintArray JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_get (JNIEnv *env, jobject jobj, jint javaKey){

    unsigned int key = (unsigned int) javaKey;

    auto sit = map->find(key);

    jintArray iarr = nullptr;

    int *foundValues;

    if(sit != map->end()) {

        iarr = env->NewIntArray(sit->second.size() * 2);
        foundValues = (int *)malloc(sizeof(int) * sit->second.size() * 2);
        //std::cerr << "Number of items for key: " << key << " is " << sit->second.size() << std::endl;
        int i;

        for(i = 0; i < sit->second.size(); ++i) {
            //consume(pos);
            //std::cerr << "Item is: " << sit->second[i].tgt << "---" << sit->second[i].win << std::endl;
            foundValues[2*i] = (int)sit->second[i].tgt;
            foundValues[2*i+1] = (int)sit->second[i].win;
            //++i;
        }

        env->SetIntArrayRegion(iarr, 0, sit->second.size() * 2, foundValues);

        free(foundValues);

    }
    //else {
    //    std::cerr << "No bucket for key: " << key << std::endl;
    //}

    return iarr;

}

JNIEXPORT jintArray JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_keys (JNIEnv *env, jobject jobj) {
    jintArray iarr = env->NewIntArray(map->size());

    int *keys = (int *)malloc(sizeof(int) * map->size());

    std::unordered_map<unsigned, std::vector<location>>::iterator it = map->begin();

    unsigned i = 0;
    for(it = map->begin(); it != map->end(); ++it) {
        keys[i] = it->first;
        ++i;
    }

    env->SetIntArrayRegion(iarr, 0, map->size(), keys);

    free(keys);

    return iarr;

}

JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_post_1process (JNIEnv *env, jobject jobj, jboolean over_populated, jboolean ambiguous) {
/*
    if (over_populated) {
        for (unsigned current_value: marked_for_deletion) {
            auto sid = map->find(current_value);

                if (sid != map->end()) {

                    sid->second.clear();
                    map->erase(current_value);
                }

        }

    }
    else {

        std::unordered_map<unsigned, std::vector<location>>::iterator current_it;

        for (current_it = map->begin(); current_it != map->end(); ++current_it) {

            if (current_it->second.size() > 254) {
                current_it->second.resize(254);
            }

        }
    }

    return marked_for_deletion.size();
    */

    return 1;

}

JNIEXPORT void JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_clear (JNIEnv *env, jobject jobj) {

    for(auto bucket = map->begin(); bucket!=map->end(); ++bucket) {

        bucket->second.clear();


    }

    map->clear();

    //delete map;

  }

JNIEXPORT void JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_clear_1key (JNIEnv *env, jobject jobj, jint javaKey) {

    unsigned int key = (unsigned int) javaKey;

    auto sit = map->find(key);

    if(sit != map->end()) {

        sit->second.clear();

    }

}