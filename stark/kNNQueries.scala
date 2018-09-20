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

import scala.util.Random


/**
  * knnQueries For Different Geometric Objects.
  */

object kNNQueries extends App {

  val conf = new SparkConf().setAppName("STARK kNN Queries")
  // conf.set("spark.serializer", classOf[KryoSerializer].getName)
  // conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
  val sc = new SparkContext(conf)
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val points = "/data/points_200M.csv"
  val polygons = "/data/buildings_114M.csv"
  val rectangles = "/data/rectangles_114M.csv"
  val linestrings = "/data/linestrings_72M.csv"

  val numPartitions = 1024

  knnPoint()
  //knnLineString()
  //knnPolygon()
  //knnRectangle()

  sc.stop()

  

  def warmup(objectRDD: RDD[Index[(STObject, Int)]], ds: String) = {
    val rangeQueryWindow6 = new Envelope(-180.0, 180.0, -90.0, 90.0)

    for(i <- 1 to 3) {
      val t0 = System.nanoTime()
      val warmUpResults = (1 to 20).map(_ => objectRDD.coveredby(rangeQueryWindow6).count()).toList
      val t1 = System.nanoTime()
    }

    println(s"stark;knn;$ds;-;warmup;${(t1 - t0) / 1E9};-;-")
  } 

  def kNN(objectRDD: RDD[Index[(STObject, Int)]], ds: String) = {

    val random = scala.util.Random

    val nQueries = 10
    val ks = Seq(1,5,10,20,30,40,50)
    for(k <- ks) {

      val t0 = System.nanoTime()
      var count1 = 0L
      for (i <- 1 to nQueries) {
        var lat = (random.nextDouble() * 2 - 1) * 90
        var long = (random.nextDouble() * 2 - 1) * 180
        val kNNQueryPoint = STObject(long, lat)
        count1 = objectRDD.knnAgg(kNNQueryPoint, k, Distance.seuclid _).count() // flag true to use index
      }
      val t1 = System.nanoTime()
      // println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      // println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
      println(s"stark;knn;$ds;$k;exec;${(t1 - t0) / 1E9};${(nQueries * 60) / ((t1 - t0) / (1E9))};$count1")
    }
  }


  def knnPoint() {

    val objectRDD = sc.textFile(points, numPartitions).map(_.split(',')).map{ arr => (STObject(arr(0).toDouble, arr(1).toDouble), arr(0).toDouble.toInt)}
                      .index(None, RTreeConfig(order = 50))
                      .cache()

    warmup(objectRDD, "point")
    kNN(objectRDD, "point")    

  }

  def knnLineString() {

    val objectRDD = sc.textFile(linestrings, numPartitions).map{ line => (STObject(line), line.length)}
                      .index(None, RTreeConfig(order = 50))
                      .cache()

    warmup(objectRDD, "linestring")
    kNN(objectRDD, "linestring")

  }

  def knnPolygon() {

    val objectRDD = sc.textFile(polygons, numPartitions).map{ line => (STObject(line), line.length)}
                      .index(None, RTreeConfig(order = 50))
                      .cache()

    warmup(objectRDD, "polygon")
    kNN(objectRDD, "polygon")
  }

  def knnRectangle() {

    val objectRDD = sc.textFile(rectangles, numPartitions).map{ line => (STObject(line), line.length)}
                      .index(None, RTreeConfig(order = 50))
                      .cache()

    warmup(objectRD, "rectangle")
    kNN(objectRDD, "rectangle")
  }
}
