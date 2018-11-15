#!/bin/bash

INPUT_SEQUENCES=Genomica/InputDatasets/InputReads/TimingKraken/HiSeq_timing.fa
PARTITION_NUMBER=32
OUTPUT_DIR=Genomica/Output_HiSeq_Native_$PARTITION_NUMBER
DATABASE_NAME=DatabaseNative_$PARTITION_NUMBER
BUFFER_SIZE=400000
RESULTS_SIZE=32
echo "spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors $PARTITION_NUMBER --master yarn --executor-memory 15G --driver-memory 12G MetaCacheSpark-0.3.0.jar -m query -d hashmultimap_native -p $PARTITION_NUMBER -b $BUFFER_SIZE -s $RESULTS_SIZE $DATABASE_NAME $OUTPUT_DIR $INPUT_SEQUENCES"
spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors $PARTITION_NUMBER --master yarn --executor-memory 15G --driver-memory 12G MetaCacheSpark-0.3.0.jar -m query -d hashmultimap_native -p $PARTITION_NUMBER -b $BUFFER_SIZE -s $RESULTS_SIZE $DATABASE_NAME $OUTPUT_DIR $INPUT_SEQUENCES
#--conf spark.yarn.driver.memoryOverhead=1500