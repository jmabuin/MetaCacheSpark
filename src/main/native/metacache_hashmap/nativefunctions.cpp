#include "com_github_jmabuin_metacachespark_database_HashMultiMapNative.h"

/*
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
*/


#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <set>

#include "../location.h"

//HashMultiMap *map;
std::unordered_map<unsigned, std::vector<location>> *map;
std::set<unsigned> marked_for_deletion;

JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_init (JNIEnv *env, jobject jobj) {

    //map = new HashMultiMap();
    map = new std::unordered_map<unsigned, std::vector<location>>();

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

}

JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_database_HashMultiMapNative_size (JNIEnv *env, jobject jobj) {

    //return map->getNumKeys();
    return map->size();
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