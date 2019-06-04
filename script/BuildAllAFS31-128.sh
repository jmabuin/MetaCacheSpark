#!/bin/bash

INPUT_SEQUENCES=Genomica/InputDatasets/AFS31-Robin
TAXONOMY=Genomica/InputDatasets/Taxonomy
PARTITION_NUMBER=128
DATABASE_NAME=DatabaseNativeAFS31_$PARTITION_NUMBER

spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --conf spark.network.timeout=10000000 --num-executors $PARTITION_NUMBER --master yarn --executor-memory 24G --driver-memory 20G MetaCacheSpark-0.3.0.jar -m build -o -p $PARTITION_NUMBER -t $TAXONOMY $DATABASE_NAME $INPUT_SEQUENCES