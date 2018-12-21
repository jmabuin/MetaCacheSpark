#!/bin/bash

INPUT_SEQUENCES="Genomica/InputDatasets/AFS/Kal_D/raw_lane2_R1.fq Genomica/InputDatasets/AFS/Kal_D/raw_lane2_R2.fq"
PARTITION_NUMBER=64
OUTPUT_DIR=Genomica/Output_Kal_D_$PARTITION_NUMBER
DATABASE_NAME=DatabaseNativeAFSBig_$PARTITION_NUMBER
BUFFER_SIZE=500000
RESULTS_SIZE=32
echo "spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors $PARTITION_NUMBER --master yarn --executor-memory 25G --driver-memory 20G MetaCacheSpark-0.3.0.jar -m query -d hashmultimap_native -p $PARTITION_NUMBER -b $BUFFER_SIZE -s $RESULTS_SIZE -r $DATABASE_NAME $OUTPUT_DIR $INPUT_SEQUENCES"
spark-submit --class com.github.jmabuin.metacachespark.MetaCacheSpark --num-executors $PARTITION_NUMBER --master yarn --executor-memory 25G --driver-memory 20G MetaCacheSpark-0.3.0.jar -m query -d hashmultimap_native -p $PARTITION_NUMBER -b $BUFFER_SIZE -s $RESULTS_SIZE -r $DATABASE_NAME $OUTPUT_DIR $INPUT_SEQUENCES
#--conf spark.yarn.driver.memoryOverhead=1500
