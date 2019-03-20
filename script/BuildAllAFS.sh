#!/bin/bash

for i in 1 2 3
do
   ./BuildAllAFS20-32Ex.sh
   ./BuildAllAFS20-64Ex.sh
   ./BuildAllAFS20-128Ex.sh
   ./BuildAllAFS20RS90-64Ex.sh
   ./BuildAllAFS20RS90-128Ex.sh
   ./BuildAllAFS31-64.sh
   ./BuildAllAFS31RS90-64.sh

   hdfs dfs -rm -r DatabaseNativeAFS*

done