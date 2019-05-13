#!/bin/bash

INPUT_SEQUENCES=Genomica/InputDatasets/AFS31RS90/
TAXONOMY=Genomica/InputDatasets/Taxonomy
PARTITION_NUMBER=64
DATABASE_NAME=DatabaseNativeAFS31RS90_$PARTITION_NUMBER

spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --conf spark.network.timeout=10000000 --num-executors $PARTITION_NUMBER --master yarn --executor-memory 48G --driver-memory 30G MetaCacheSpark-0.3.0.jar -m build -d hashmultimap_native -w -p $PARTITION_NUMBER -t $TAXONOMY $DATABASE_NAME $INPUT_SEQUENCES