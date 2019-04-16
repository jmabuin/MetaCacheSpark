#!/bin/bash

for i in 1 2
do
   ./BuildAllAFS20-MC-32.sh
   ./BuildAllAFS20-MC-64.sh
   #./BuildAllAFS20-128.sh
   ./BuildAllAFS20RS90-MC-32.sh
   ./BuildAllAFS20RS90-MC-64.sh
   #./BuildAllAFS20RS90-128.sh
   ./BuildAllAFS31-MC-32.sh
   ./BuildAllAFS31-MC-64.sh
   #./BuildAllAFS31-128.sh
   ./BuildAllAFS31RS90-MC-32.sh
   ./BuildAllAFS31RS90-MC-64.sh
   #./BuildAllAFS31RS90-128.sh

   hdfs dfs -rm -r DatabaseNativeAFS20-MC*
   hdfs dfs -rm -r DatabaseNativeAFS20RS90-MC*
   hdfs dfs -rm -r DatabaseNativeAFS31-MC*
   hdfs dfs -rm -r DatabaseNativeAFS31RS90-MC*

done

./BuildAllAFS20-MC-32.sh
./BuildAllAFS20-MC-64.sh
#./BuildAllAFS20-128.sh
./BuildAllAFS20RS90-MC-32.sh
./BuildAllAFS20RS90-MC-64.sh
#./BuildAllAFS20RS90-128.sh
./BuildAllAFS31-MC-32.sh
./BuildAllAFS31-MC-64.sh
#./BuildAllAFS31-128.sh
./BuildAllAFS31RS90-MC-32.sh
./BuildAllAFS31RS90-MC-64.sh
#./BuildAllAFS31RS90-128.sh
