#!/bin/bash

for i in 1 2
do
    ./BuildAllAFS20-8.sh
    ./BuildAllAFS20-16.sh
    ./BuildAllAFS20-32.sh
    ./BuildAllAFS20-64.sh
    ./BuildAllAFS20-128.sh
    ./BuildAllAFS20RS90-8.sh
    ./BuildAllAFS20RS90-16.sh
    ./BuildAllAFS20RS90-32.sh
    ./BuildAllAFS20RS90-64.sh
    ./BuildAllAFS20RS90-128.sh
    ./BuildAllAFS31-8.sh
    ./BuildAllAFS31-16.sh
    ./BuildAllAFS31-32.sh
    ./BuildAllAFS31-64.sh
    #./BuildAllAFS31-128.sh
    ./BuildAllAFS31RS90-8.sh
    ./BuildAllAFS31RS90-16.sh
    ./BuildAllAFS31RS90-32.sh
    ./BuildAllAFS31RS90-64.sh
    #./BuildAllAFS31RS90-128.sh

    hdfs dfs -rm -r DatabaseNativeAFS20_*
    hdfs dfs -rm -r DatabaseNativeAFS20RS90_*
    hdfs dfs -rm -r DatabaseNativeAFS31_*
    hdfs dfs -rm -r DatabaseNativeAFS31RS90_*

done

./BuildAllAFS20-8.sh
./BuildAllAFS20-16.sh
./BuildAllAFS20-32.sh
./BuildAllAFS20-64.sh
./BuildAllAFS20-128.sh
./BuildAllAFS20RS90-8.sh
./BuildAllAFS20RS90-16.sh
./BuildAllAFS20RS90-32.sh
./BuildAllAFS20RS90-64.sh
./BuildAllAFS20RS90-128.sh
./BuildAllAFS31-8.sh
./BuildAllAFS31-16.sh
./BuildAllAFS31-32.sh
./BuildAllAFS31-64.sh
#./BuildAllAFS31-128.sh
./BuildAllAFS31RS90-8.sh
./BuildAllAFS31RS90-16.sh
./BuildAllAFS31RS90-32.sh
./BuildAllAFS31RS90-64.sh
#./BuildAllAFS31RS90-128.sh

