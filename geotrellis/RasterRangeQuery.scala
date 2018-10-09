package geotrellis

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.raster.mapalgebra.local._
import geotrellis.spark.io._

import astraea.spark.rasterframes._
import org.apache.spark.sql._



object RasterRangeQuery {


  def main(args: Array[String]): Unit = {

    implicit val spark = SparkSession.builder()
                          .appName("RasterFrames").
                          getOrCreate().
                          withRasterFrames


    import spark.implicits._
  }


  def rangeQuery(rdd: String, range: String) {

    val data = sc.textFile(rdd).map{ line =>
      val arr = line.split(',')

      val col = arr(0).toInt
      val row = arr(1).toInt
      val ulx = arr(2).toDouble
      val uly = arr(3).toDouble
      val width = arr(4).toInt
      val height = arr(5).toInt

      val d = new Array[Double](arr.length - 6)

      var i = 0
      while(i < d.length) {
        d(i) = arr(i+6).toDouble
        i += 1
      }

      (SpatialKey(col, row), DoubleArrayTile(d, width, height))
    }.toRF(3600,1800)

    data.show()
  }

}