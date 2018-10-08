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

import org.apache.spark.serializer.KryoSerializer
import dbis.stark.StarkKryoRegistrator

import scala.util.Random

import dbis.stark.spatial.partitioner._
/**
  * knnQueries For Different Geometric Objects.
  */

object kNNQueries extends App {

  val conf = new SparkConf().setAppName("STARK kNN Queries")
  conf.set("spark.serializer", classOf[KryoSerializer].getName)
  conf.set("spark.kryo.registrator", classOf[StarkKryoRegistrator].getName)
  val sc = new SparkContext(conf)
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val dummy: Byte = 0

  val points = "/data/points_200M.csv"
  val polygons = "/data/buildings_114M.csv"
  val rectangles = "/data/rectangles_114M.csv"
  val linestrings = "/data/linestrings_72M.csv"

  val numPartitions = 1024

  knnPoint()
  knnLineString()
  knnPolygon()
  knnRectangle()


  sc.stop()

  

  def warmup(objectRDD: RDD[Index[(STObject, Byte)]], ds: String) = {
    val rangeQueryWindow6 = STObject(new Envelope(-180.0, 180.0, -90.0, 90.0))

    var t0 = 0L
    var t1 = 0L
    // for(i <- 1 to 3) {
      t0 = System.nanoTime()
      val warmUpResults = (1 to 5).map(_ => objectRDD.coveredby(rangeQueryWindow6).count()).toList
      t1 = System.nanoTime()
    // }

    println(s"stark;knn;$ds;-;warmup;${(t1 - t0) / 1E9};-;-")
  } 

  def kNN(objectRDD: RDD[Index[(STObject, Byte)]], ds: String) = {

    val random = new scala.util.Random(100)

    val nQueries = 10
    val ks = Seq(1,5,10,20,30,40,50)

    var lat = (random.nextDouble() * 2 - 1) * 90
    var long = (random.nextDouble() * 2 - 1) * 180
    val kNNQueryPoint = STObject(long, lat)
    System.err.println(s"$ds : $lat $long")

    for(k <- ks) {

      val t0 = System.nanoTime()
      var count1 = 0L
      for (i <- 1 to nQueries) {
        
        count1 = objectRDD.knnAgg(kNNQueryPoint, k, Distance.seuclid _).count() // flag true to use index
      }
      val t1 = System.nanoTime()
      // println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      // println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
      println(s"stark;knnagg;$ds;$k;exec;${((t1 - t0) / 1E9)/nQueries};${(nQueries * 60) / ((t1 - t0) / (1E9))};$count1")
    }

    // for(k <- ks) {

    //   val t0 = System.nanoTime()
    //   var count1 = 0L
    //   for (i <- 1 to nQueries) {
        
    //     count1 = objectRDD.knnTake(kNNQueryPoint, k, Distance.seuclid _).count() // flag true to use index
    //   }
    //   val t1 = System.nanoTime()
    //   // println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    //   // println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    //   println(s"stark;knntake;$ds;$k;exec;${((t1 - t0) / 1E9)/nQueries};${(nQueries * 60) / ((t1 - t0) / (1E9))};$count1")
    // }

    // for(k <- ks) {

    //   val t0 = System.nanoTime()
    //   var count1 = 0L
    //   for (i <- 1 to nQueries) {
        
    //     count1 = objectRDD.kNN(kNNQueryPoint, k, Distance.seuclid _).count() // flag true to use index
    //   }
    //   val t1 = System.nanoTime()
    //   // println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    //   // println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    //   println(s"stark;knn;$ds;$k;exec;${((t1 - t0) / 1E9)/nQueries};${(nQueries * 60) / ((t1 - t0) / (1E9))};$count1")
    // }
  }


  def knnPoint() {

    val objectRDD = sc.textFile(points, numPartitions).map(_.split(',')).map{ arr => (STObject(arr(0).toDouble, arr(1).toDouble), dummy)}
                      // .index(RTreeStrategy(1024, true,minmax = Some((-180, 180.0001, -90, 90.0001)), sampleFraction=0.001), RTreeConfig(order = 50))
                      .index(None, RTreeConfig(order = 50))
                      .cache()

    warmup(objectRDD, "point")
    kNN(objectRDD, "point")    

  }

  def knnLineString() {

    val objectRDD = sc.textFile(linestrings, numPartitions).map{ line => (STObject(line), dummy)}
                      // .index(RTreeStrategy(1024, true,minmax = Some((-180, 180.0001, -90, 90.0001)), sampleFraction=0.001), RTreeConfig(order = 50))
                      .index(None, RTreeConfig(order = 50))
                      .cache()

    warmup(objectRDD, "linestring")
    kNN(objectRDD, "linestring")

  }

  def knnPolygon() {

    val objectRDD = sc.textFile(polygons, numPartitions).map{ line => (STObject(line), dummy)}
                      // .index(RTreeStrategy(1024, true,minmax = Some((-180, 180.0001, -90, 90.0001)), sampleFraction=0.001), RTreeConfig(order = 50))
                      .index(None, RTreeConfig(order = 50))
                      .cache()

    warmup(objectRDD, "polygon")
    kNN(objectRDD, "polygon")
  }

  def knnRectangle() {

    val objectRDD = sc.textFile(rectangles, numPartitions).map{ line => (STObject(line), dummy)}
                      // .index(RTreeStrategy(1024, true,minmax = Some((-180, 180.0001, -90, 90.0001)), sampleFraction=0.001), RTreeConfig(order = 50))
                      .index(None, RTreeConfig(order = 50))
                      .cache()

    warmup(objectRDD, "rectangle")
    kNN(objectRDD, "rectangle")
  }
}
