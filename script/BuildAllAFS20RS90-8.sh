#!/bin/bash

INPUT_SEQUENCES=Genomica/InputDatasets/AFS20RS90-Robin
TAXONOMY=Genomica/InputDatasets/Taxonomy
PARTITION_NUMBER=8
DATABASE_NAME=DatabaseNativeAFS20RS90_$PARTITION_NUMBER

#spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors $PARTITION_NUMBER --master yarn --executor-memory 30G --driver-memory 12G MetaCacheSpark-0.3.0.jar -m build -w -p $PARTITION_NUMBER -t $TAXONOMY $DATABASE_NAME $INPUT_SEQUENCES
spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --conf spark.network.timeout=10000000  --num-executors $PARTITION_NUMBER --master yarn --executor-memory 200G --driver-memory 35G MetaCacheSpark-0.3.0.jar -m build -o -p $PARTITION_NUMBER -t $TAXONOMY $DATABASE_NAME $INPUT_SEQUENCES