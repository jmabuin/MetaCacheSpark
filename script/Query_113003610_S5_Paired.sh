#!/bin/bash

INPUT_SEQUENCES="Genomica/InputDatasets/AFS/SausageData/113003610_S5_forward_paired.fq Genomica/InputDatasets/AFS/SausageData/113003610_S5_reverse_paired.fq"
PARTITION_NUMBER=32
OUTPUT_DIR=Genomica/Output_113003610_S5_$PARTITION_NUMBER
DATABASE_NAME=DatabaseNativeAFS_$PARTITION_NUMBER
BUFFER_SIZE=500000
RESULTS_SIZE=32
echo "spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors $PARTITION_NUMBER --master yarn --executor-memory 25G --driver-memory 20G MetaCacheSpark-0.3.0.jar -m query -d hashmultimap_native -p $PARTITION_NUMBER -b $BUFFER_SIZE -s $RESULTS_SIZE -r $DATABASE_NAME $OUTPUT_DIR $INPUT_SEQUENCES"
spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors $PARTITION_NUMBER --master yarn --executor-memory 25G --driver-memory 20G MetaCacheSpark-0.3.0.jar -m query -d hashmultimap_native -p $PARTITION_NUMBER -b $BUFFER_SIZE -s $RESULTS_SIZE -r $DATABASE_NAME $OUTPUT_DIR $INPUT_SEQUENCES
#--conf spark.yarn.driver.memoryOverhead=1500
