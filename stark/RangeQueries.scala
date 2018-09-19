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
  class StatsListener extends SparkListener {

    var maxPeakMemory = 0L
    var shuffleWrite = 0L
    var shuffleRead = 0L
    var spilled = 0L

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
      val metrics = stageCompleted.stageInfo.taskMetrics

      maxPeakMemory = math.max(maxPeakMemory, metrics.peakExecutionMemory)
      shuffleWrite += metrics.shuffleWriteMetrics.bytesWritten
      shuffleRead += metrics.shuffleReadMetrics.localBytesRead + metrics.shuffleReadMetrics.remoteBytesRead
      spilled += metrics.memoryBytesSpilled
    }

    def reset() = {
      maxPeakMemory = 0L
      shuffleRead = 0L
      shuffleWrite = 0L
      spilled = 0L
    }
  }

object RangeQueries extends App {

  val conf = new SparkConf().setAppName("STARK Range Queries")
   conf.set("spark.serializer", classOf[KryoSerializer].getName)
   conf.set("spark.kryo.registrator", classOf[StarkKryoRegistrator].getName)
  conf.set("spark.ui.showConsoleProgress","false")
  val sc = new SparkContext(conf)
  
  val metrics = new StatsListener()
  sc.addSparkListener(metrics)

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

  // spatialRangePoint()
  // spatialRangeLineString()
  // spatialRangeRectangle()
  // spatialRangePolygon()

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

  

  def loadRDD(rdd: String, parti: String) = {

    val raw = rdd match {
      case "point" => sc.textFile(points, numInitPartitions).map(_.split(',')).map{ arr => (STObject(arr(0).toDouble, arr(1).toDouble), arr(0).toDouble.toInt)}
      case "linestring" => sc.textFile(linestrings, numInitPartitions).map{ line => (STObject(line), line.length)}
      case "rectangle" => sc.textFile(rectangles, numInitPartitions).map{ line => (STObject(line), line.length)}
      case "polygon" => sc.textFile(polygons, numInitPartitions).map{ line => (STObject(line), line.length)}
    }

    val partitioner: Option[dbis.stark.spatial.partitioner.PartitionerConfig] = parti match {
      case "grid" => Some(GridStrategy(numInitPartitions, pointsOnly = rdd == "point", minmax = Some((-180, 180.0001, -90, 90.0001)), sampleFraction = 0.01 ))
      case "bsp" => Some(BSPStrategy(0.5, 100000, pointsOnly = rdd == "point",minmax = Some((-180, 180.0001, -90, 90.0001)), sampleFraction=0.01))
      case "rtree" => Some(RTreeStrategy(1024, pointsOnly = rdd == "point",minmax = Some((-180, 180.0001, -90, 90.0001)), sampleFraction=0.001))
      case _ => None
    }

    raw.index(indexer, partitioner).cache()
  }


  def bm(ds: String, ranges: Map[String, STObject], parti: String) = {
    val objectRDD = loadRDD(ds, parti)
                      // .index(parti, indexer)
                      // .partitionBy(parti)
                      // .cache()

    var t0 = 0L
    var t1 = 0L
    var count1 = 0L
    var count = 0L

    // Materialize IndexedRDD
    t0 = System.nanoTime()
    for (i <- 1 to warmupQueries) {
      metrics.reset()
      count = objectRDD.coveredby(rangeQueryWindow6).count()
    }
    t1 = System.nanoTime()

    // platform,querytype,range,total time,throughput,count,sel ratio
    println(s"stark;rangequery;$ds;$parti;warmup;${(t1 - t0) / 1E9};-;$count;100;${metrics.maxPeakMemory};${metrics.shuffleWrite};${metrics.shuffleRead};${metrics.spilled}")


    for(range <- ranges) {
      // Actual Measurements
      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        metrics.reset()
        count1 = objectRDD.coveredby(range._2).count()
      }
      t1 = System.nanoTime()

      println(s"stark;rangequery;$ds;$parti;${range._1};${(t1 - t0) / 1E9};${(nQueries * 60) / ((t1 - t0) / (1E9))};$count1;${((count1 * 100.0) / count)};${metrics.maxPeakMemory};${metrics.shuffleWrite};${metrics.shuffleRead};${metrics.spilled}")
    }

    

    objectRDD.unpersist()
  }


  // def spatialRangePoint() {

  //   val rangeQueryWindow1 = STObject(new Envelope(-50.3010141441, -24.9526465797, -53.209588996, -30.1096863746))
  //   val rangeQueryWindow2 = STObject(new Envelope(-54.4270741441, -24.9526465797, -53.209588996, -30.1096863746))
  //   val rangeQueryWindow3 = STObject(new Envelope(-114.4270741441, 42.9526465797, -54.509588996, -27.0106863746))
  //   val rangeQueryWindow4 = STObject(new Envelope(-82.7638020000, 42.9526465797, -54.509588996, 38.0106863746))
  //   val rangeQueryWindow5 = STObject(new Envelope(-140.99778, 5.7305630159, -52.6480987209, 83.23324))
  //   val rangeQueryWindow6 = STObject(new Envelope(-180.0, 180.0, -90.0, 90.0))

  //   println("************************ POINT Range Queries **************************************")

    

  //   val objectRDD = loadRDD("point")
  //                     .index(parti, indexer)
  //                     // .partitionBy(parti)
  //                     .cache()

  //   var t0 = 0L
  //   var t1 = 0L
  //   var count1 = 0L
  //   var count = 0L

  //   // Materialize IndexedRDD
  //   t0 = System.nanoTime()
  //   for (i <- 1 to warmupQueries) {
  //     count = objectRDD.coveredby(rangeQueryWindow6).count()
  //   }
  //   t1 = System.nanoTime()

  //   // platform,querytype,range,total time,throughput,count,sel ratio
  //   println(s"stark;rangequery;warmup;${(t1 - t0) / 1E9};-;$count;100")

  //   // println(s"WarumUp ${(t1 - t0) / 1E9} sec -- count: $count")
  //   // println(s"Total count: ${objectRDD.count()}")

  //   // Actual Measurements
  //   // println("Range1: ")
  //   t0 = System.nanoTime()
  //   for (i <- 1 to nQueries) {
  //     count1 = objectRDD.coveredby(rangeQueryWindow1).count()
  //   }
  //   t1 = System.nanoTime()
  //   // println("Count: " + count1)
  //   // println("Selection Ratio: " + ((count1 * 100.0) / count))
  //   // println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
  //   // println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")

  //   println(s"stark;rangequery;range1;${(t1 - t0) / 1E9};${(nQueries * 60) / ((t1 - t0) / (1E9))};$count1;${((count1 * 100.0) / count)}")

  //   t1 = 0L
  //   t0 = 0L

  //   println("Range2: ")
  //   t0 = System.nanoTime()
  //   for (i <- 1 to nQueries) {
  //     count1 = objectRDD.coveredby(rangeQueryWindow2).count()
  //   }
  //   t1 = System.nanoTime()
  //   println("Count: " + count1)
  //   println("Selection Ratio: " + ((count1 * 100.0) / count))
  //   println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
  //   println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
  //   t1 = 0L
  //   t0 = 0L

  //   println("Range3: ")
  //   t0 = System.nanoTime()
  //   for (i <- 1 to nQueries) {
  //     count1 = objectRDD.coveredby(rangeQueryWindow3).count()
  //   }
  //   t1 = System.nanoTime()
  //   println("Count: " + count1)
  //   println("Selection Ratio: " + ((count1 * 100.0) / count))
  //   println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
  //   println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
  //   t1 = 0L
  //   t0 = 0L

  //   println("Range4: ")
  //   t0 = System.nanoTime()
  //   for (i <- 1 to nQueries) {
  //     count1 = objectRDD.coveredby(rangeQueryWindow4).count()
  //   }
  //   t1 = System.nanoTime()
  //   println("Count: " + count1)
  //   println("Selection Ratio: " + ((count1 * 100.0) / count))
  //   println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
  //   println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
  //   t1 = 0L
  //   t0 = 0L

  //   println("Range5: ")
  //   t0 = System.nanoTime()
  //   for (i <- 1 to nQueries) {
  //     count1 = objectRDD.coveredby(rangeQueryWindow5).count()
  //   }
  //   t1 = System.nanoTime()
  //   println("Count: " + count1)
  //   println("Selection Ratio: " + ((count1 * 100.0) / count))
  //   println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
  //   println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
  //   t1 = 0L
  //   t0 = 0L

  //   println("Range6: ")
  //   t0 = System.nanoTime()
  //   for (i <- 1 to nQueries) {
  //     count1 = objectRDD.coveredby(rangeQueryWindow6).count()
  //   }
  //   t1 = System.nanoTime()
  //   println("Count: " + count1)
  //   println("Selection Ratio: " + ((count1 * 100.0) / count))
  //   println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
  //   println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
  //   t1 = 0L
  //   t0 = 0L


  //   objectRDD.unpersist()

  //   println("***********************************************************************************")
  //   println("")
  // }

  // def spatialRangeLineString() {

  //   val rangeQueryWindow1 = STObject(new Envelope(-50.204, -24.9526465797, -53.209588996, -30.1096863746))
  //   val rangeQueryWindow2 = STObject(new Envelope(-52.1270741441, -24.9526465797, -53.209588996, -30.1096863746))
  //   val rangeQueryWindow3 = STObject(new Envelope(-94.4270741441, 22.9526465797, -34.609588996, -27.0106863746))
  //   val rangeQueryWindow4 = STObject(new Envelope(-74.0938020000, 42.9526465797, -54.509588996, 38.0106863746))
  //   val rangeQueryWindow5 = STObject(new Envelope(-150.99778, 7.2705630159, -52.6480987209, 83.23324))
  //   val rangeQueryWindow6 = STObject(new Envelope(-180.0, 180.0, -90.0, 90.0))

  //   println("************************ LineString Range Queries **************************************")

  //   val objectRDD = loadRDD("linestring")
  //                     .index(parti, indexer)
  //                     .cache()

  //   var t0 = 0L
  //   var t1 = 0L
  //   var count1 = 0L
  //   var count = 0L



  //   // Materialize RDDs
  //   t0 = System.nanoTime()
  //   for (i <- 1 to warmupQueries) {
  //     count = objectRDD.coveredby(rangeQueryWindow6).count()
  //   }
  //   t1 = System.nanoTime()

  //   println(s"WarumUp count: $count")
  //   println(s"Total count: ${objectRDD.count()}")


  //   // Actual Measurements
  //   println("Range1: ")
  //   t0 = System.nanoTime()
  //   for (i <- 1 to nQueries) {
  //     count1 = objectRDD.coveredby(rangeQueryWindow1).count()
  //   }
  //   t1 = System.nanoTime()
  //   println("Count: " + count1)
  //   println("Selection Ratio: " + ((count1 * 100.0) / count))
  //   println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
  //   println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
  //   t1 = 0L
  //   t0 = 0L

  //   println("Range2: ")
  //   t0 = System.nanoTime()
  //   for (i <- 1 to nQueries) {
  //     count1 = objectRDD.coveredby(rangeQueryWindow2).count()
  //   }
  //   t1 = System.nanoTime()
  //   println("Count: " + count1)
  //   println("Selection Ratio: " + ((count1 * 100.0) / count))
  //   println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
  //   println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
  //   t1 = 0L
  //   t0 = 0L

  //   println("Range3: ")
  //   t0 = System.nanoTime()
  //   for (i <- 1 to nQueries) {
  //     count1 = objectRDD.coveredby(rangeQueryWindow3).count()
  //   }
  //   t1 = System.nanoTime()
  //   println("Count: " + count1)
  //   println("Selection Ratio: " + ((count1 * 100.0) / count))
  //   println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
  //   println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
  //   t1 = 0L
  //   t0 = 0L

  //   println("Range4: ")
  //   t0 = System.nanoTime()
  //   for (i <- 1 to nQueries) {
  //     count1 = objectRDD.coveredby(rangeQueryWindow4).count()
  //   }
  //   t1 = System.nanoTime()
  //   println("Count: " + count1)
  //   println("Selection Ratio: " + ((count1 * 100.0) / count))
  //   println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
  //   println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
  //   t1 = 0L
  //   t0 = 0L

  //   println("Range5: ")
  //   t0 = System.nanoTime()
  //   for (i <- 1 to nQueries) {
  //     count1 = objectRDD.coveredby(rangeQueryWindow5).count()
  //   }
  //   t1 = System.nanoTime()
  //   println("Count: " + count1)
  //   println("Selection Ratio: " + ((count1 * 100.0) / count))
  //   println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
  //   println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
  //   t1 = 0L
  //   t0 = 0L

  //   println("Range6: ")
  //   t0 = System.nanoTime()
  //   for (i <- 1 to nQueries) {
  //     count1 = objectRDD.coveredby(rangeQueryWindow6).count()
  //   }
  //   t1 = System.nanoTime()
  //   println("Count: " + count1)
  //   println("Selection Ratio: " + ((count1 * 100.0) / count))
  //   println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
  //   println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
  //   t1 = 0L
  //   t0 = 0L

  //   objectRDD.unpersist()

  //   println("***********************************************************************************")
  //   println("")
  // }

  // def spatialRangePolygon() {

  //   val rangeQueryWindow1 = STObject(new Envelope(-20.204, 17.9526465797, -53.209588996, -30.1096863746))
  //   val rangeQueryWindow2 = STObject(new Envelope(-20.204, 20.4376465797, -53.209588996, -30.1096863746))
  //   val rangeQueryWindow3 = STObject(new Envelope(-74.4270741441, 72.9526465797, -34.609588996, -6.5906863746))
  //   val rangeQueryWindow4 = STObject(new Envelope(-104.0938020000, 118.9526465797, -54.509588996, 40.2406863746))
  //   val rangeQueryWindow5 = STObject(new Envelope(-174.4270741441, 72.9526465797, -34.609588996, 48.4396863746))
  //   val rangeQueryWindow6 = STObject(new Envelope(-180.0, 180.0, -90.0, 90.0))

  //   println("************************ POLYGON Range Queries **************************************")

  //   val objectRDD = loadRDD("polygon")
  //                     .index(parti, indexer)
  //                     .cache()

  //   var t0 = 0L
  //   var t1 = 0L
  //   var count1 = 0L
  //   var count = 0L

  //   // Materialize RDDs
  //   t0 = System.nanoTime()
  //   for (i <- 1 to warmupQueries) {
  //     count = objectRDD.coveredby(rangeQueryWindow6).count()
  //   }
  //   t1 = System.nanoTime()

  //   // Actual Measurements
  //   println("Range1: ")
  //   t0 = System.nanoTime()
  //   for (i <- 1 to nQueries) {
  //     count1 = objectRDD.coveredby(rangeQueryWindow1).count()
  //   }
  //   t1 = System.nanoTime()
  //   println("Count: " + count1)
  //   println("Selection Ratio: " + ((count1 * 100.0) / count))
  //   println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
  //   println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
  //   t1 = 0L
  //   t0 = 0L

  //   println("Range2: ")
  //   t0 = System.nanoTime()
  //   for (i <- 1 to nQueries) {
  //     count1 = objectRDD.coveredby(rangeQueryWindow2).count()
  //   }
  //   t1 = System.nanoTime()
  //   println("Count: " + count1)
  //   println("Selection Ratio: " + ((count1 * 100.0) / count))
  //   println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
  //   println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
  //   t1 = 0L
  //   t0 = 0L

  //   println("Range3: ")
  //   t0 = System.nanoTime()
  //   for (i <- 1 to nQueries) {
  //     count1 = objectRDD.coveredby(rangeQueryWindow3).count()
  //   }
  //   t1 = System.nanoTime()
  //   println("Count: " + count1)
  //   println("Selection Ratio: " + ((count1 * 100.0) / count))
  //   println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
  //   println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
  //   t1 = 0L
  //   t0 = 0L

  //   println("Range4: ")
  //   t0 = System.nanoTime()
  //   for (i <- 1 to nQueries) {
  //     count1 = objectRDD.coveredby(rangeQueryWindow4).count()
  //   }
  //   t1 = System.nanoTime()
  //   println("Count: " + count1)
  //   println("Selection Ratio: " + ((count1 * 100.0) / count))
  //   println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
  //   println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
  //   t1 = 0L
  //   t0 = 0L

  //   println("Range5: ")
  //   t0 = System.nanoTime()
  //   for (i <- 1 to nQueries) {
  //     count1 = objectRDD.coveredby(rangeQueryWindow5).count()
  //   }
  //   t1 = System.nanoTime()
  //   println("Count: " + count1)
  //   println("Selection Ratio: " + ((count1 * 100.0) / count))
  //   println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
  //   println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
  //   t1 = 0L
  //   t0 = 0L

  //   println("Range6: ")
  //   t0 = System.nanoTime()
  //   for (i <- 1 to nQueries) {
  //     count1 = objectRDD.coveredby(rangeQueryWindow6).count()
  //   }
  //   t1 = System.nanoTime()
  //   println("Count: " + count1)
  //   println("Selection Ratio: " + ((count1 * 100.0) / count))
  //   println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
  //   println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
  //   t1 = 0L
  //   t0 = 0L

  //   objectRDD.unpersist()

  //   println("***********************************************************************************")
  //   println("")
  // }

  // def spatialRangeRectangle() {

  //   val rangeQueryWindow1 = STObject(new Envelope(-20.204, 17.9526465797, -53.209588996, -30.1096863746))
  //   val rangeQueryWindow2 = STObject(new Envelope(-20.204, 20.4376465797, -53.209588996, -30.1096863746))
  //   val rangeQueryWindow3 = STObject(new Envelope(-74.4270741441, 72.9526465797, -34.609588996, -6.5906863746))
  //   val rangeQueryWindow4 = STObject(new Envelope(-104.0938020000, 118.9526465797, -54.509588996, 40.2406863746))
  //   val rangeQueryWindow5 = STObject(new Envelope(-174.4270741441, 72.9526465797, -34.609588996, 48.4396863746))
  //   val rangeQueryWindow6 = STObject(new Envelope(-180.0, 180.0, -90.0, 90.0))

  //   println("************************ Rectangle Range Queries **************************************")

  //   val objectRDD = loadRDD("rectangle")
  //                     .index(parti, indexer)
  //                     .cache()

  //   var t0 = 0L
  //   var t1 = 0L
  //   var count1 = 0L
  //   var count = 0L
  //   t0 = System.nanoTime()

  //   // Materialize RDDs
  //   for (i <- 1 to warmupQueries) {
  //     count = objectRDD.coveredby(rangeQueryWindow6).count()
  //   }
  //   t1 = System.nanoTime()

  //   // Actual Measurements
  //   println("Range1: ")
  //   t0 = System.nanoTime()
  //   for (i <- 1 to nQueries) {
  //     count1 = objectRDD.coveredby(rangeQueryWindow1).count()
  //   }
  //   t1 = System.nanoTime()
  //   println("Count: " + count1)
  //   println("Selection Ratio: " + ((count1 * 100.0) / count))
  //   println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
  //   println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
  //   t1 = 0L
  //   t0 = 0L

  //   println("Range2: ")
  //   t0 = System.nanoTime()
  //   for (i <- 1 to nQueries) {
  //     count1 = objectRDD.coveredby(rangeQueryWindow2).count()
  //   }
  //   t1 = System.nanoTime()
  //   println("Count: " + count1)
  //   println("Selection Ratio: " + ((count1 * 100.0) / count))
  //   println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
  //   println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
  //   t1 = 0L
  //   t0 = 0L

  //   println("Range3: ")
  //   t0 = System.nanoTime()
  //   for (i <- 1 to nQueries) {
  //     count1 = objectRDD.coveredby(rangeQueryWindow3).count()
  //   }
  //   t1 = System.nanoTime()
  //   println("Count: " + count1)
  //   println("Selection Ratio: " + ((count1 * 100.0) / count))
  //   println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
  //   println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
  //   t1 = 0L
  //   t0 = 0L

  //   println("Range4: ")
  //   t0 = System.nanoTime()
  //   for (i <- 1 to nQueries) {
  //     count1 = objectRDD.coveredby(rangeQueryWindow4).count()
  //   }
  //   t1 = System.nanoTime()
  //   println("Count: " + count1)
  //   println("Selection Ratio: " + ((count1 * 100.0) / count))
  //   println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
  //   println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
  //   t1 = 0L
  //   t0 = 0L

  //   println("Range5: ")
  //   t0 = System.nanoTime()
  //   for (i <- 1 to nQueries) {
  //     count1 = objectRDD.coveredby(rangeQueryWindow5).count()
  //   }
  //   t1 = System.nanoTime()
  //   println("Count: " + count1)
  //   println("Selection Ratio: " + ((count1 * 100.0) / count))
  //   println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
  //   println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
  //   t1 = 0L
  //   t0 = 0L

  //   println("Range6: ")
  //   t0 = System.nanoTime()
  //   for (i <- 1 to nQueries) {
  //     count1 = objectRDD.coveredby(rangeQueryWindow6).count()
  //   }
  //   t1 = System.nanoTime()
  //   println("Count: " + count1)
  //   println("Selection Ratio: " + ((count1 * 100.0) / count))
  //   println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
  //   println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
  //   t1 = 0L
  //   t0 = 0L

  //   objectRDD.unpersist()

  //   println("***********************************************************************************")
  //   println("")
  // }
}
