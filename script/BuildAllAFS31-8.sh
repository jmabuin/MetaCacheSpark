#!/bin/bash

INPUT_SEQUENCES=Genomica/InputDatasets/AFS31-Robin
TAXONOMY=Genomica/InputDatasets/Taxonomy
PARTITION_NUMBER=8
DATABASE_NAME=DatabaseNativeAFS31_$PARTITION_NUMBER

spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --conf spark.network.timeout=10000000 --num-executors $PARTITION_NUMBER --master yarn --executor-memory 200G --driver-memory 35G MetaCacheSpark-0.3.0.jar -m build -d hashmultimap_native -o -p $PARTITION_NUMBER -t $TAXONOMY $DATABASE_NAME $INPUT_SEQUENCES