#!/bin/bash

INPUT_SEQUENCES=Genomica/InputDatasets/AFS/RefSeq
TAXONOMY=Genomica/InputDatasets/RefSeq90/Refseq-Release90-Taxonomy
PARTITION_NUMBER=32
DATABASE_NAME=DatabaseNativeAFS_Combine_$PARTITION_NUMBER

spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors $PARTITION_NUMBER --master yarn --executor-memory 25G --driver-memory 20G MetaCacheSpark-0.3.0.jar -m build -d combine -w -p $PARTITION_NUMBER -t $TAXONOMY $DATABASE_NAME $INPUT_SEQUENCES