package stark.measurements

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.locationtech.jts.geom.Envelope

import dbis.stark.STObject
import dbis.stark.Distance
import org.apache.spark.SpatialRDD._
import dbis.stark.spatial.indexed._
import dbis.stark.spatial.Skyline

import org.apache.spark.serializer.KryoSerializer
import dbis.stark.StarkKryoRegistrator

import scala.util.Random

import dbis.stark.spatial.partitioner._
/**
  * knnQueries For Different Geometric Objects.
  */

object SkylineQueries extends App {

  val conf = new SparkConf().setAppName("STARK kNN Queries")
  conf.set("spark.serializer", classOf[KryoSerializer].getName)
  conf.set("spark.kryo.registrator", classOf[StarkKryoRegistrator].getName)
  val sc = new SparkContext(conf)
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val dummy: Byte = 0

  val sentinel = "/data/sentinel2.wkt"
  val gdelt = "/data/gdelt/events_2013-04-01_TO_2015-10-18.tsv"

  val numPartitions = 1024

  skylineSentinel()
  skylineGdelt()

  sc.stop()

  

  def warmup(objectRDD: RDD[(STObject, Byte)], ds: String) = {
    val rangeQueryWindow6 = STObject(new Envelope(-180.0, 180.0, -90.0, 90.0))

    var t0 = 0L
    var t1 = 0L
    // for(i <- 1 to 3) {
      t0 = System.nanoTime()
      val warmUpResults = (1 to 5).map(_ => objectRDD.coveredby(rangeQueryWindow6).count()).toList
      t1 = System.nanoTime()
    // }

    println(s"stark;skyline;$ds;warmup;${(t1 - t0) / 1E9};-;-")
  } 

  def skyline(objectRDD: RDD[(STObject, Byte)], ds: String) = {

    val random = new scala.util.Random(100)

    val nQueries = 10

    var lat = (random.nextDouble() * 2 - 1) * 90
    var long = (random.nextDouble() * 2 - 1) * 180

    val time = random.nextInt(1538983201).toLong

    val kNNQueryPoint = STObject(long, lat, time)
    System.err.println(s"$ds : ($lat $long) $time")


    var t0 = System.nanoTime()
    var count1 = 0L
    for (i <- 1 to nQueries) {
      
      count1 = objectRDD.skyline(kNNQueryPoint, Distance.euclid, Skyline.centroidDominates, ppD = 32, allowCache = true).count() 
    }
    var t1 = System.nanoTime()
    println(s"stark;skyline;$ds;exec;${((t1 - t0) / 1E9)/nQueries};${(nQueries * 60) / ((t1 - t0) / (1E9))};$count1")


    t0 = System.nanoTime()
    count1 = 0L
    for (i <- 1 to nQueries) {
      count1 = objectRDD.skylineAgg(kNNQueryPoint, Distance.euclid, Skyline.centroidDominates).count() 
    }
    t1 = System.nanoTime()
    println(s"stark;skylineagg;$ds;exec;${((t1 - t0) / 1E9)/nQueries};${(nQueries * 60) / ((t1 - t0) / (1E9))};$count1")


    t0 = System.nanoTime()
    count1 = 0L
    for (i <- 1 to nQueries) {
      count1 = objectRDD.skylineAngular(kNNQueryPoint, Distance.euclid, Skyline.centroidDominates, ppd = 1024).count() 
    }
    t1 = System.nanoTime()
    println(s"stark;skylineangular;$ds;exec;${((t1 - t0) / 1E9)/nQueries};${(nQueries * 60) / ((t1 - t0) / (1E9))};$count1")

    t0 = System.nanoTime()
    count1 = 0L
    for (i <- 1 to nQueries) {
      count1 = objectRDD.skylineAngular2(kNNQueryPoint, Distance.euclid, Skyline.centroidDominates, ppd = 32).count() 
    }
    t1 = System.nanoTime()
    println(s"stark;skylineangular2;$ds;exec;${((t1 - t0) / 1E9)/nQueries};${(nQueries * 60) / ((t1 - t0) / (1E9))};$count1")

  }


  def skylineSentinel() {

    val objectRDD = sc.textFile(sentinel, numPartitions).map(_.split(';')).map{ arr => 
      (STObject(arr(1), arr(2).toLong, arr(3).toLong), dummy)
    }
                      // .index(RTreeStrategy(1024, true,minmax = Some((-180, 180.0001, -90, 90.0001)), sampleFraction=0.001), RTreeConfig(order = 50))
                      // .index(None, RTreeConfig(order = 50))
                      // .cache()

    // warmup(objectRDD, "point")
    skyline(objectRDD, "sentinel")    

  }

  def skylineGdelt() {

    val objectRDD = sc.textFile(gdelt, numPartitions)
                      .map(_.split("\t"))
                      .filter{arr => arr(1).length == 8 && arr(53) != "" && arr(54) != "" }
                      .map{ arr => 

                        val timeCol = arr(1)
                        val year = timeCol.substring(0,4).toInt
                        val month = timeCol.substring(4,6).toInt
                        val day = timeCol.substring(6).toInt

                        val date = java.time.LocalDate.of(year, month,day)
                        val epoch = date.atStartOfDay(java.time.ZoneId.systemDefault()).toEpochSecond();
                        
                        (STObject(arr(53).toDouble, arr(54).toDouble, epoch), dummy)
                      }
                      // .index(RTreeStrategy(1024, true,minmax = Some((-180, 180.0001, -90, 90.0001)), sampleFraction=0.001), RTreeConfig(order = 50))
                      // .index(None, RTreeConfig(order = 50))
                      // .cache()

    // warmup(objectRDD, "point")
    skyline(objectRDD, "gdelt")    

  }




  
}
