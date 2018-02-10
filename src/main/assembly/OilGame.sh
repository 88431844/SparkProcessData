#!/bin/sh
JAR_PATH=lib/OilGame.jar

for jar in `ls lib/*.jar`
do
  if [ $jar != $JAR_PATH ] ; then
    LIBJARS=$jar,$LIBJARS
  fi
done

spark-submit --class com.navinfo.qingqi.spark.ranking.OilGame  \
--master spark://10.30.50.20:7085 \
--executor-memory 1G \
--driver-memory 2G \
--conf spark.default.parallelism=100 \
--total-executor-cores 30 \
--executor-cores 2 \
--deploy-mode client --jars  $LIBJARS  $JAR_PATH $day
