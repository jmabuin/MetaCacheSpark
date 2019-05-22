#!/bin/bash

for i in 1 2
do
   ./BuildAllAFS20-32.sh
   ./BuildAllAFS20-64.sh
   ./BuildAllAFS20-128.sh
   ./BuildAllAFS20RS90-32.sh
   ./BuildAllAFS20RS90-64.sh
   ./BuildAllAFS20RS90-128.sh
   ./BuildAllAFS31-32.sh
   ./BuildAllAFS31-64.sh
   ./BuildAllAFS31-128.sh
   ./BuildAllAFS31RS90-32.sh
   ./BuildAllAFS31RS90-64.sh
   ./BuildAllAFS31RS90-128.sh

   hdfs dfs -rm -r DatabaseNativeAFS*

done

./BuildAllAFS20-32.sh
./BuildAllAFS20-64.sh
./BuildAllAFS20-128.sh
./BuildAllAFS20RS90-32.sh
./BuildAllAFS20RS90-64.sh
./BuildAllAFS20RS90-128.sh
./BuildAllAFS31-32.sh
./BuildAllAFS31-64.sh
./BuildAllAFS31-128.sh
./BuildAllAFS31RS90-32.sh
./BuildAllAFS31RS90-64.sh
./BuildAllAFS31RS90-128.sh
