#!/bin/bash

INPUT_SEQUENCES=Genomica/InputDatasets/InputReads/TimingKraken/HiSeq_timing.fa
PARTITION_NUMBER=32
OUTPUT_DIR=Genomica/Output_HiSeq_HashMap_$PARTITION_NUMBER
DATABASE_NAME=DatabaseHashMap_$PARTITION_NUMBER
BUFFER_SIZE=150000
RESULTS_SIZE=32
echo "spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors $PARTITION_NUMBER --master yarn --executor-memory 30G --driver-memory 12G MetaCacheSpark-0.3.0.jar -m query -d hashmap -p $PARTITION_NUMBER -b $BUFFER_SIZE -s $RESULTS_SIZE $DATABASE_NAME $OUTPUT_DIR $INPUT_SEQUENCES"
spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors $PARTITION_NUMBER --master yarn --executor-memory 30G --driver-memory 12G MetaCacheSpark-0.3.0.jar -m query -d hashmap -p $PARTITION_NUMBER -b $BUFFER_SIZE -s $RESULTS_SIZE $DATABASE_NAME $OUTPUT_DIR $INPUT_SEQUENCES
#--conf spark.yarn.driver.memoryOverhead=1500