#!/bin/sh

THECLASS=$1

FRAMES=/home/hg/code/rasterframes/core/target/scala-2.11/rasterframes_2.11-0.7.2-SNAPSHOT.jar
GEOTRELLIS=/home/hg/code/geotrellis/raster/target/scala-2.11/geotrellis-raster_2.11-2.0.0-SNAPSHOT.jar
GEOTRELLIS_SPARK=/home/hg/code/geotrellis/spark/target/scala-2.11/geotrellis-spark_2.11-2.0.0-SNAPSHOT.jar

pushd geotrellis

scalac -d geotrellis_runner.jar -cp .:$FRAMES:$GEOTRELLIS:$GEOTRELLIS_SPARK:/home/hage/code/lib/spark-2.1.0-bin-hadoop2.7/jars/* ${THECLASS}.scala
result=$?

popd
if [ "$result" != 0 ]
then
        echo "exit code of scala compiler $result"
        exit $result
fi

# --deploy-mode cluster
# spark-submit --master yarn  --jars $FRAMES,$GEOTRELLIS,$GEOTRELLIS_SPARK --num-executors 16 --executor-cores 3 --executor-memory 12g --class geotrellis.measurements.$THECLASS geotrellis/geotrellis_runner.jar
