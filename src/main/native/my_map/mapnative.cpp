#include "com_github_jmabuin_metacachespark_database_MapNative.h"
#include "../location.h"

#include <map>
#include <unordered_map>
#include <functional>
#include <set>

std::map<location, unsigned int> *myMap;
std::map<location, unsigned int>::iterator myIterator;
std::map<location, unsigned int>::iterator FSTIterator;
std::map<location, unsigned int>::iterator LSTIterator;

std::map<location, unsigned int> *tmp_map;

JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_database_MapNative_init (JNIEnv *env, jobject jobj) {

    myMap = new std::map<location, unsigned int>();
    myIterator = myMap->begin();
    FSTIterator = myMap->begin();
    LSTIterator = myMap->begin();

    tmp_map = new std::map<location, unsigned int>();

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

        //myMap->at(newLocation) = myMap->at(newLocation) + 1;
        it->second = it->second + 1;

    }
    else {

        //if(myMap->size() < 256) {
            myMap->insert(std::pair<location, unsigned int>(newLocation, 1));
        //}
    }

    return myMap->size();

}

JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_database_MapNative_addAll (JNIEnv *env, jobject jobj, jintArray data) {

    const jsize arr_length = env->GetArrayLength(data);
    jint *arr = env->GetIntArrayElements(data, nullptr);

    std::map<location,unsigned int>::iterator it;

    for (int i = 0; i< arr_length; i+=2) {
        location new_location{arr[i], arr[i+1]};

        it = tmp_map->find(new_location);

            if(it != tmp_map->end()) {

                //tmp_map->at(new_location) = tmp_map->at(new_location) + 1;
                it->second = it->second + 1;

            }
            else {

                //if(myMap->size() < 256) {
                    tmp_map->insert(std::pair<location, unsigned int>(new_location, 1));
                //}
            }


    }

    env->ReleaseIntArrayElements(data, arr, NULL);

    return tmp_map->size();
}

JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_database_MapNative_get
  (JNIEnv *env, jobject jobj, jint tgt, jint win) {

    location newLocation;

    newLocation.tgt = (unsigned int) tgt;
    newLocation.win = (unsigned int) win;


    return (int) myMap->at(newLocation);

}

JNIEXPORT jintArray JNICALL Java_com_github_jmabuin_metacachespark_database_MapNative_get_1best (JNIEnv *env, jobject jobj, jint size) {
    jintArray iarr = env->NewIntArray(size * 3);

        int *foundValues = (int *)malloc(sizeof(int) * size * 3);

    typedef std::function<bool(std::pair<location, unsigned int>, std::pair<location,unsigned int>)> Comparator;

    	// Defining a lambda function to compare two pairs. It will compare two pairs using second field
    	Comparator compFunctor =
    			[](std::pair<location, unsigned int> elem1 ,std::pair<location, unsigned int> elem2)
    			{
    				return elem1.second > elem2.second;
    			};

    	// Declaring a set that will store the pairs using above comparision logic
    	std::set<std::pair<location, unsigned int>, Comparator> setOfValues(
    			tmp_map->begin(), tmp_map->end(), compFunctor);

    	// Iterate over a set using range base for loop
    	// It will display the items in sorted order of values
    	int current_number_of_items = 0;
    	int current_item = 0;
    	for (std::pair<location, unsigned int> element : setOfValues) {
    		//std::cout << element.first << " :: " << element.second << std::endl;
            foundValues[current_item] = element.first.tgt;
            foundValues[current_item +1] = element.first.win;
            foundValues[current_item+2] = element.second;

            current_item +=3;
            ++current_number_of_items;

            if (current_number_of_items >= size) {

                break;
            }

    		}
        env->SetIntArrayRegion(iarr, 0, size * 3, foundValues);
        free(foundValues);


        return iarr;
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
  tmp_map->clear();

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