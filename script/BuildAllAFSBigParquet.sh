#!/bin/bash

INPUT_SEQUENCES=Genomica/InputDatasets/BigDataset/
TAXONOMY=Genomica/InputDatasets/RefSeq90/Refseq-Release90-Taxonomy
PARTITION_NUMBER=64
DATABASE_NAME=DatabaseNativeAFS_Parquet_$PARTITION_NUMBER

spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors $PARTITION_NUMBER --conf spark.network.timeout=10000000 --master yarn --executor-memory 30G --driver-memory 20G MetaCacheSpark-0.3.0.jar -m build -d parquet -w -l -p $PARTITION_NUMBER -t $TAXONOMY $DATABASE_NAME $INPUT_SEQUENCES