#!/bin/bash
src=$1
local_file=/tmp/`echo $src | awk '{split($0,a,"/"); print a[5],""}'`

echo local_file $local_file
echo src $src

if [ -f $local_file ]; then
    echo rm $local_file
     rm $local_file
     hdfs dfs -copyToLocal $src $local_file
else
     hdfs dfs -copyToLocal $src $local_file

fi


file=$local_file
spark-submit --conf spark.executor.memory=$2g --conf spark.akka.frameSize=1024 --conf spark.driver.memory=$3g --conf spark.dynamicAllocation.initialExecutors=$4 --conf spark.dynamicAllocation.maxExecutors=$5 --conf spark.executor.extraJavaOptions='"'-XX:MaxPermSize=1024M'"' --conf spark.kryoserializer.buffer.max=1024m  $file
