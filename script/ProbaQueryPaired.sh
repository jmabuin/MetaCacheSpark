#!/bin/bash

INPUT_SEQUENCES="Genomica/InputDatasets/AFS/SausageData/Input_S4_Forward.fq Genomica/InputDatasets/AFS/SausageData/Input_S4_Reverse.fq"
PARTITION_NUMBER=64
OUTPUT_DIR=Genomica/ProbaPaired_$PARTITION_NUMBER
DATABASE_NAME=DatabaseNativeAFS20_$PARTITION_NUMBER
BUFFER_SIZE=400000
EXECUTOR_MEM="40G"
BUFFER_SIZE=500000
echo "spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors $PARTITION_NUMBER --master yarn --executor-memory 15G --driver-memory 12G MetaCacheSpark-0.3.0.jar -m query -d hashmultimap_native -p $PARTITION_NUMBER -b $BUFFER_SIZE -s $RESULTS_SIZE -r $DATABASE_NAME $OUTPUT_DIR $INPUT_SEQUENCES"
spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors $PARTITION_NUMBER --master yarn --executor-memory $EXECUTOR_MEM --driver-memory 30G MetaCacheSpark-0.3.0.jar -m query -d hashmultimap_native -p $PARTITION_NUMBER -a species -o -b $BUFFER_SIZE -r $DATABASE_NAME $OUTPUT_DIR $INPUT_SEQUENCES
#--conf spark.yarn.driver.memoryOverhead=1500