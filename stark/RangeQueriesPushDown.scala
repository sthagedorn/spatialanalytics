package stark.measurements

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

import org.locationtech.jts.geom.Envelope

import dbis.stark.STObject
import org.apache.spark.SpatialRDD._
import dbis.stark.spatial.partitioner._

import org.apache.spark.serializer.KryoSerializer
import dbis.stark.StarkKryoRegistrator

import org.apache.spark.scheduler._

object RangeQueries extends App {

  val conf = new SparkConf().setAppName("STARK Range Queries w/ Pushdown")
   conf.set("spark.serializer", classOf[KryoSerializer].getName)
   conf.set("spark.kryo.registrator", classOf[StarkKryoRegistrator].getName)
  conf.set("spark.ui.showConsoleProgress","false")
  val sc = new SparkContext(conf)
  
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val points = "/data/points_200M.csv"
  val polygons = "/data/buildings_114M.csv"
  val rectangles = "/data/rectangles_114M.csv"
  val linestrings = "/data/linestrings_72M.csv"

  val numPartitions = 32
  val numInitPartitions = 1024 //4*numPartitions

  // val parti = BSPStrategy(0.5, 100000, true,minmax = Some((-180, 180.0001, -90, 90.0001)), sampleFactor=0.2)
  // val parti = GridStrategy(numInitPartitions, pointsOnly = true, minmax = Some((-180, 180.0001, -90, 90.0001)) )
  val parti: Option[SpatialPartitioner] = None
  val indexer = dbis.stark.spatial.indexed.RTreeConfig(order = 10)


  val nQueries = 10
  val warmupQueries = 3

  val rangeQueryWindow1 = STObject(new Envelope(-50.3010141441, -24.9526465797, -53.209588996, -30.1096863746))
  val rangeQueryWindow2 = STObject(new Envelope(-54.4270741441, -24.9526465797, -53.209588996, -30.1096863746))
  val rangeQueryWindow3 = STObject(new Envelope(-114.4270741441, 42.9526465797, -54.509588996, -27.0106863746))
  val rangeQueryWindow4 = STObject(new Envelope(-82.7638020000, 42.9526465797, -54.509588996, 38.0106863746))
  val rangeQueryWindow5 = STObject(new Envelope(-140.99778, 5.7305630159, -52.6480987209, 83.23324))
  val rangeQueryWindow6 = STObject(new Envelope(-180.0, 180.0, -90.0, 90.0))

  val ranges = Map(
    "range1" -> rangeQueryWindow1,
    "range2" -> rangeQueryWindow2,
    "range3" -> rangeQueryWindow3,
    "range4" -> rangeQueryWindow4,
    "range5" -> rangeQueryWindow5,
    "range6" -> rangeQueryWindow6
  )

  val datasets = Seq("point", "linestring", "rectangle","polygon")

  for(dataset <- datasets) {
    bm(dataset, ranges, "rtree")
  }
  
  sc.stop()

  

  def loadRDD(rdd: String, parti: String, qry: STObject) = {

    
    val dir = "/data/stark_partitioned/"+rdd

    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val exists = fs.exists(new org.apache.hadoop.fs.Path(dir))

    if(!exists) {
      val raw = rdd match {
        case "point" => sc.textFile(points, numInitPartitions).map(_.split(',')).map{ arr => (STObject(arr(0).toDouble, arr(1).toDouble), arr(0).toDouble.toInt)}
        case "linestring" => sc.textFile(linestrings, numInitPartitions).map{ line => (STObject(line), line.length)}
        case "rectangle" => sc.textFile(rectangles, numInitPartitions).map{ line => (STObject(line), line.length)}
        case "polygon" => sc.textFile(polygons, numInitPartitions).map{ line => (STObject(line), line.length)}
      }

      val partitioner: dbis.stark.spatial.partitioner.PartitionerConfig = parti match {
        case "grid" => GridStrategy(numInitPartitions, pointsOnly = rdd == "point", minmax = Some((-180, 180.0001, -90, 90.0001)), sampleFraction = 0.01 )
        case "bsp" => BSPStrategy(0.5, 100000, pointsOnly = rdd == "point",minmax = Some((-180, 180.0001, -90, 90.0001)), sampleFraction=0.01)
        case "rtree" => RTreeStrategy(1024, pointsOnly = rdd == "point",minmax = Some((-180, 180.0001, -90, 90.0001)), sampleFraction=0.001)
        case _ => ???
      }

      raw.partitionBy(partitioner).saveAsStarkObjectFile(dir)
    }

    val ctx = new dbis.stark.spatial.STSparkContext(sc)

    val raw = ctx.objectFile[(STObject,Int)](dir, qry)

    // rdd match {
    //     case "point" => raw.map(_.split(',')).map{ arr => (STObject(arr(0).toDouble, arr(1).toDouble), arr(0).toDouble.toInt)}
    //     case "linestring" => raw.map{ line => (STObject(line), line.length)}
    //     case "rectangle" => raw.map{ line => (STObject(line), line.length)}
    //     case "polygon" => raw.map{ line => (STObject(line), line.length)}
    // }

    raw

  }


  def bm(ds: String, ranges: Map[String, STObject], parti: String) = {

    var t0 = 0L
    var t1 = 0L
    var count1 = 0L
    var count = 0L

    // Materialize IndexedRDD
    // t0 = System.nanoTime()
    // for (i <- 1 to warmupQueries) {
    //   // metrics.reset()
    //   count = objectRDD.coveredby(rangeQueryWindow6).count()
    // }
    // t1 = System.nanoTime()

    // // platform,querytype,range,total time,throughput,count,sel ratio
    // println(s"stark;rangequery;$ds;$parti;warmup;${(t1 - t0) / 1E9};-;$count;100")


    for(range <- ranges) {
      // Actual Measurements
      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        // metrics.reset()
        count1 = loadRDD(ds, parti, range._2).coveredby(range._2).count()
      }
      t1 = System.nanoTime()

      println(s"stark;rangequery;$ds;$parti;${range._1};${(t1 - t0) / 1E9};${(nQueries * 60) / ((t1 - t0) / (1E9))};$count1;${((count1 * 100.0) / count)}")
    }

    

    // objectRDD.unpersist()
  }
}
