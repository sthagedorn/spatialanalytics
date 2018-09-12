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

  

  def warmup(objectRDD: RDD[Index[(STObject, Int)]]) = {
    val rangeQueryWindow6 = new Envelope(-180.0, 180.0, -90.0, 90.0)
    val t0 = System.nanoTime()
    val warmUpResults = (1 to 20).map(_ => objectRDD.coveredby(rangeQueryWindow6).count()).toList
    val t1 = System.nanoTime()
    println("distinct warmup count results ${warmUpResults.distinct.length}")
    println("warm up took ${(t1 - t0) / 1E9} sec")
  } 

  def kNN(objectRDD: RDD[Index[(STObject, Int)]]) = {

    val random = scala.util.Random

    val nQueries = 100
    val ks = Seq(1,5,10,20,30,40,50)
    for(k <- ks) {

      println(s"k=$k")
      
      val t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        var lat = (random.nextDouble() * 2 - 1) * 90
        var long = (random.nextDouble() * 2 - 1) * 180
        val kNNQueryPoint = STObject(long, lat)
        val count1 = objectRDD.kNN(kNNQueryPoint, k, Distance.seuclid _).count() // flag true to use index
      }
      val t1 = System.nanoTime()
      println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    }
  }


  def knnPoint() {

    println("************************ POINT KNN Queries **************************************")

    val objectRDD = sc.textFile(points, numPartitions).map(_.split(',')).map{ arr => (STObject(arr(0).toDouble, arr(1).toDouble), arr(0).toDouble.toInt)}
                      .index(None, RTreeConfig(order = 5))
                      .cache()

    warmup(objectRDD)
    kNN(objectRDD)    

    println("***********************************************************************************")
    println("")
  }

  def knnLineString() {

    println("************************ LineString KNN Queries **************************************")
    val objectRDD = sc.textFile(linestrings, numPartitions).map{ line => (STObject(line), line.length)}
                      .index(None, RTreeConfig(order = 5))
                      .cache()

    warmup(objectRDD)
    kNN(objectRDD)

    println("***********************************************************************************")
    println("")
  }

  def knnPolygon() {

    println("************************ POLYGON KNN Queries **************************************")

    val objectRDD = sc.textFile(polygons, numPartitions).map{ line => (STObject(line), line.length)}
                      .index(None, RTreeConfig(order = 5))
                      .cache()

    warmup(objectRDD)
    kNN(objectRDD)

    println("***********************************************************************************")
    println("")
  }

  def knnRectangle() {

    println("************************ Rectangle KNN Queries **************************************")

    val objectRDD = sc.textFile(rectangles, numPartitions).map{ line => (STObject(line), line.length)}
                      .index(None, RTreeConfig(order = 5))
                      .cache()

    warmup(objectRDD)
    kNN(objectRDD)
    println("***********************************************************************************")
    println("")
  }
}
