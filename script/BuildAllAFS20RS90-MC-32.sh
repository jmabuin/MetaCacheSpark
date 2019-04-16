#!/bin/bash

INPUT_SEQUENCES=Genomica/InputDatasets/BigDataset/
TAXONOMY=Genomica/InputDatasets/RefSeq90/Refseq-Release90-Taxonomy
PARTITION_NUMBER=32
DATABASE_NAME=DatabaseNativeAFS20RS90-MC_$PARTITION_NUMBER

#spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors $PARTITION_NUMBER --master yarn --executor-memory 30G --driver-memory 12G MetaCacheSpark-0.3.0.jar -m build -d hashmultimap_native -w -p $PARTITION_NUMBER -t $TAXONOMY $DATABASE_NAME $INPUT_SEQUENCES
spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --conf spark.network.timeout=10000000  --num-executors $PARTITION_NUMBER --master yarn --executor-memory 75G --driver-memory 20G MetaCacheSpark-0.3.0.jar -m build -d hashmultimap_native -w -l -p $PARTITION_NUMBER -t $TAXONOMY $DATABASE_NAME $INPUT_SEQUENCES
