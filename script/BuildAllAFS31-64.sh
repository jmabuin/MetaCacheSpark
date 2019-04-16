#!/bin/bash

INPUT_SEQUENCES=Genomica/InputDatasets/AFS31/
TAXONOMY=Genomica/InputDatasets/RefSeq90/Refseq-Release90-Taxonomy
PARTITION_NUMBER=64
DATABASE_NAME=DatabaseNativeAFS31_$PARTITION_NUMBER

spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --conf spark.network.timeout=10000000 --num-executors $PARTITION_NUMBER --master yarn --executor-memory 45G --driver-memory 25G MetaCacheSpark-0.3.0.jar -m build -d hashmultimap_native -w -p $PARTITION_NUMBER -t $TAXONOMY $DATABASE_NAME $INPUT_SEQUENCES