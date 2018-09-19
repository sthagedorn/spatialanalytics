package stark.measurements

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

import dbis.stark.STObject
import org.apache.spark.SpatialRDD._
import dbis.stark.spatial.partitioner._
import dbis.stark.spatial.JoinPredicate

import org.apache.spark.serializer.KryoSerializer
import dbis.stark.StarkKryoRegistrator

object SpatialJoins {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("STARK Spatial Joins")
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[StarkKryoRegistrator].getName)

    val sc = new SparkContext(conf)
    
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val points = "/data/points_200M.csv"
    val polygons = "/data/buildings_114M.csv"
    val rectangles = "/data/rectangles_114M.csv"
    val linestrings = "/data/linestrings_72M.csv"


    val numPartitions = 32*32
    val numInitPartitions = 1024 //4*numPartitions

    // val parti = BSPStrategy(0.5, 100000, true,minmax = Some((-180, 180.0001, -90, 90.0001)), sampleFactor=0.2)
    // val parti = GridStrategy(numInitPartitions, pointsOnly = true, minmax = Some((-180, 180.0001, -90, 90.0001)) )
    
    // val parti: Option[SpatialPartitioner] = None
    val indexer = dbis.stark.spatial.indexed.RTreeConfig(order = 50)

    val nQueries = 1
    val warmupQueries = 0

    val partitioners = Seq(PartitionStrategy.RTREE)

    for(j <- 1 to nQueries) {
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
    }

    sc.stop()




    def runSpatialJoin(partitioningScheme: PartitionStrategy.PartitionStrategy, leftrdd: String, rightrdd: String) {

      // var count = 0L
      val beginTime = System.currentTimeMillis()
      var t0 = 0L
      var t1 = 0L

      // println("******************************** " + leftrdd + " and " + rightrdd + " spatial join ********************************")

      t0 = System.nanoTime()
      val leftRDD = leftrdd match {

        case "point" => sc.textFile(points, numPartitions).map(_.split(',')).map{ arr => (STObject(arr(0).toDouble, arr(1).toDouble), arr(0).toDouble.toInt)}
        case "linestring" => sc.textFile(linestrings, numPartitions).map{ line => (STObject(line), line.length)}
        case "rectangle" => sc.textFile(rectangles, numPartitions).map{ line => (STObject(line), line.length)}
        case "polygon" => sc.textFile(polygons, numPartitions).map{ line => (STObject(line), line.length)}

      }

      //partitioning
      val leftParti: PartitionerConfig =  partitioningScheme match {
        case PartitionStrategy.GRID =>
          GridStrategy(32, leftrdd == "point",minmax = Some((-180, 180.0001, -90, 90.0001)), sampleFraction=0.01) 
        case PartitionStrategy.BSP =>
          BSPStrategy(0.1, 100000, leftrdd == "point",minmax = Some((-180, 180.0001, -90, 90.0001)), sampleFraction=0.01)
        case PartitionStrategy.RTREE =>
          RTreeStrategy(1024, leftrdd == "point",minmax = Some((-180, 180.0001, -90, 90.0001)), sampleFraction=0.001)
        }




      val leftRDDPartiAndIndex = leftRDD //Partitioned
                                       .partitionBy(leftParti)
                                      // .partitionBy(new RTreePartitioner(leftRDD.sample(false, 0.001).collect().toList,-180, 180.0001, -90, 90.0001,32*32))
                                      // .index(None, indexer)
                                      // .index(leftParti, indexer)
                                      // .cache()

      // var sParti = leftRDDPartiAndIndex.partitioner.get.asInstanceOf[dbis.stark.spatial.partitioner.GridPartitioner]
      // println(s"LEFT: \t x: ${sParti.minX} -- ${sParti.maxX} \t y: ${sParti.minY} -- ${sParti.maxY}")

      // leftRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY)
      // leftRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
      // val c1 = leftRDD.spatialPartitionedRDD.count()
      // val c2 = leftRDD.indexedRDD.count()
      // leftRDD.rawSpatialRDD.unpersist()
      // leftRDD.spatialPartitionedRDD.unpersist()

      val rightRDD = rightrdd match {

        case "point" => sc.textFile(points, numPartitions).map(_.split(',')).map{ arr => (STObject(arr(0).toDouble, arr(1).toDouble), arr(0).toDouble.toInt)}
        case "linestring" => sc.textFile(linestrings, numPartitions).map{ line => (STObject(line), line.length)}
        case "rectangle" => sc.textFile(rectangles, numPartitions).map{ line => (STObject(line), line.length)}
        case "polygon" => sc.textFile(polygons, numPartitions).map{ line => (STObject(line), line.length)}

      }

      //partitioning
      val rightParti = partitioningScheme match {
        case PartitionStrategy.GRID =>
          GridStrategy(100, rightrdd == "point",minmax = Some((-180, 180.0001, -90, 90.0001)), sampleFraction=0.01) 
        case PartitionStrategy.BSP =>
          BSPStrategy(0.1, 100000, rightrdd == "point",minmax = Some((-180, 180.0001, -90, 90.0001)), sampleFraction=0.01)
        case PartitionStrategy.RTREE =>
          RTreeStrategy(1024, rightrdd == "point",minmax = Some((-180, 180.0001, -90, 90.0001)), sampleFraction=0.0001)
        }

      val rightRDDPartitioned = rightRDD.partitionBy(rightParti)
        // .partitionBy(new RTreePartitioner(rightRDD.sample(false, 0.001).collect().toList,-180, 180.0001, -90, 90.0001,32*32))
        // .cache()

      // val sParti = rightRDDPartitioned.partitioner.get.asInstanceOf[dbis.stark.spatial.partitioner.GridPartitioner]
      // sParti.printPartitions("/tmp/parts_polygons.wkt")
      // println(s"RIGHT: \t x: ${sParti.minX} -- ${sParti.maxX} \t y: ${sParti.minY} -- ${sParti.maxY}")

        var countL = 0L
        var countR = 0L
       t0 = System.nanoTime()
       for (i <- 1 to warmupQueries) {
          // println(s"left: ${leftRDDPartiAndIndex.count()}")
          // println(s"right: ${rightRDDPartitioned.count()}")
          countL = leftRDDPartiAndIndex.count()
          countR = rightRDDPartitioned.count()
       }
       t1 = System.nanoTime()

      //  println(s"WarumUp ${(t1 - t0) / 1E9} sec")
      println(s"stark;join;$leftrdd;$rightrdd;$partitioningScheme;prepare;${(t1 - t0) / 1E9}")

      t0 = System.nanoTime()

// 
      val count = leftRDDPartiAndIndex.liveIndex(indexer).join(rightRDDPartitioned, JoinPredicate.COVEREDBY, oneToMany = true).count()
      //JoinQuery.SpatialJoinQuery(leftRDD, rightRDD, true, false).count()


      t1 = System.nanoTime()
      val join_time = (t1 - t0) / 1E9
      // println(s"Join Time: $join_time sec ... Count: $count")

      println(s"stark;join;$leftrdd;$rightrdd;$partitioningScheme;exec;${(t1 - t0) / 1E9}")

      // val total_time = /*read_time +*/ leftPTime + rightPTime + join_time

      // println("Total Join Time: " + total_time + " sec")

      // println("********************************************************************************************")

    }
  }



}
