#!/bin/bash

INPUT_SEQUENCES=Genomica/InputDatasets/AFS31-Robin
TAXONOMY=Genomica/InputDatasets/Taxonomy
PARTITION_NUMBER=16
DATABASE_NAME=DatabaseNativeAFS31_$PARTITION_NUMBER

spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --conf spark.network.timeout=10000000 --num-executors $PARTITION_NUMBER --master yarn --executor-memory 100G --driver-memory 25G MetaCacheSpark-0.4.0.jar -m build -o -p $PARTITION_NUMBER -t $TAXONOMY $DATABASE_NAME $INPUT_SEQUENCES