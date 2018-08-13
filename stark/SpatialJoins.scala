package stark.measurements

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

import dbis.stark.STObject
import dbis.stark.spatial.SpatialRDD._
import dbis.stark.spatial.partitioner._
import dbis.stark.spatial.JoinPredicate

object SpatialJoins {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("STARK Spatial Joins")
    // conf.set("spark.serializer", classOf[KryoSerializer].getName)
    // conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
    val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.INFO)
    Logger.getLogger("akka").setLevel(Level.INFO)

    val points = "/data/points_200M.csv"
    val polygons = "/data/buildings_114M.csv"
    val rectangles = "/data/rectangles_114M.csv"
    val linestrings = "/data/linestrings_72M.csv"


    val partitioners = Seq(PartitionStrategy.GRID, PartitionStrategy.BSP)
    val numPartitions = 32

    partitioners.foreach{ p =>
      runSpatialJoin(p, "point", "point")
      runSpatialJoin(p, "point", "linestring")
      runSpatialJoin(p, "point", "polygon")
      runSpatialJoin(p, "point", "rectangle")
      runSpatialJoin(p, "linestring", "linestring")
      runSpatialJoin(p, "linestring", "polygon")
      runSpatialJoin(p, "linestring", "rectangle")
      runSpatialJoin(p, "rectangle", "rectangle")
      runSpatialJoin(p, "rectangle", "polygon")
      runSpatialJoin(p, "polygon", "polygon")
    }


    sc.stop()






    def runSpatialJoin(partitioningScheme: PartitionStrategy.PartitionStrategy, leftrdd: String, rightrdd: String) {

      // var count = 0L
      val beginTime = System.currentTimeMillis()
      var t0 = 0L
      var t1 = 0L

      println("******************************** " + leftrdd + " and " + rightrdd + " spatial join ********************************")

      t0 = System.nanoTime()
      val leftRDD = leftrdd match {

        case "point" => sc.textFile(points, numPartitions).map(_.split(';')).map{ arr => (STObject(arr(0)), arr(1))}
        case "linestring" => sc.textFile(linestrings, numPartitions).map(_.split(';')).map{ arr => (STObject(arr(0)), arr(1))}
        case "rectangle" => sc.textFile(rectangles, numPartitions).map(_.split(';')).map{ arr => (STObject(arr(0)), arr(1))}
        case "polygon" => sc.textFile(polygons, numPartitions).map(_.split(';')).map{ arr => (STObject(arr(0)), arr(1))}

      }

      //partitioning
      val leftParti = partitioningScheme match {
        case PartitionStrategy.GRID =>
          GridStrategy(30, leftrdd == "point", minmax = Some((-180, 180, -90, 90)))
        case PartitionStrategy.BSP =>
          BSPStrategy(15, 10000, leftrdd == "point", minmax = Some((-180, 180, -90, 90)))
      }

      val leftRDDPartitioned = leftRDD.partitionBy(leftParti)

      val leftRDDPartiAndIndex = leftRDDPartitioned.liveIndex(order = 10)

      // leftRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY)

      // leftRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)

      // val c1 = leftRDD.spatialPartitionedRDD.count()

      // val c2 = leftRDD.indexedRDD.count()

      // leftRDD.rawSpatialRDD.unpersist()

      // leftRDD.spatialPartitionedRDD.unpersist()

      val rightRDD = rightrdd match {

        case "point" => sc.textFile(points, numPartitions).map(_.split(';')).map{ arr => (STObject(arr(0)), arr(1))}
        case "linestring" => sc.textFile(linestrings, numPartitions).map(_.split(';')).map{ arr => (STObject(arr(0)), arr(1))}
        case "rectangle" => sc.textFile(rectangles, numPartitions).map(_.split(';')).map{ arr => (STObject(arr(0)), arr(1))}
        case "polygon" => sc.textFile(polygons, numPartitions).map(_.split(';')).map{ arr => (STObject(arr(0)), arr(1))}

      }

      //partitioning
      val rightParti = partitioningScheme match {
        case PartitionStrategy.GRID =>
          GridStrategy(30, rightrdd == "point", minmax = Some((-180, 180, -90, 90)))
        case PartitionStrategy.BSP =>
          BSPStrategy(1, 10000, rightrdd == "point", minmax = Some((-180, 180, -90, 90)))
      }

      val rightRDDPartitioned = rightRDD.partitionBy(rightParti)

      t0 = System.nanoTime()

      val count = leftRDDPartiAndIndex.join(rightRDDPartitioned, JoinPredicate.INTERSECTS).count()
      //JoinQuery.SpatialJoinQuery(leftRDD, rightRDD, true, false).count()


      t1 = System.nanoTime()
      val join_time = (t1 - t0) / 1E9
      println(s"Join Time: $join_time sec ... Count: $count")

      // val total_time = /*read_time +*/ leftPTime + rightPTime + join_time

      // println("Total Join Time: " + total_time + " sec")

      println("********************************************************************************************")

    }
  }



}