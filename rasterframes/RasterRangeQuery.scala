package rasterframes.measurements

// import geotrellis.raster._
// import geotrellis.spark._
// import geotrellis.util._
// import geotrellis.raster.mapalgebra.local._
// import geotrellis.spark.io._

// import geotrellis.raster.resample._
// import geotrellis.spark.tiling._
// import geotrellis.vector._

// import astraea.spark.rasterframes._

import astraea.spark.rasterframes._
import astraea.spark.rasterframes.datasource.geotrellis.DefaultSource._
import astraea.spark.rasterframes.util._
import astraea.spark.rasterframes.util.debug._
import geotrellis.proj4.LatLng
import geotrellis.raster._
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.resample.{NearestNeighbor, ResampleMethod}
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.avro.AvroRecordCodec
import geotrellis.spark.io.avro.codecs.Implicits._
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.tiling.ZoomedLayoutScheme
import geotrellis.vector._

import org.apache.spark.sql._
import org.apache.spark.rdd.RDD

// import astraea.spark.rasterframes.extensions.Implicits._


object RasterRangeQuery {


  def main(args: Array[String]): Unit = {

    implicit val spark = SparkSession.builder()
                          .appName("RasterFrames").
                          getOrCreate().
                          withRasterFrames


    import spark.implicits._

    // val file = "/home/hage/Documents/uni/data/raster/MOD_LSTD_CLIM_M_2001-12-01_rgb_3600x1800.TIFF"
    val file = "/data/raster/MOD_LSTD_CLIM_M_2001-12-01_rgb_3600x1800.TIFF"

    import astraea.spark.rasterframes.datasource.geotiff._

    val pt1 = Point(-88, 60)
    val pt2 = Point(-78, 38)
    val region = Extent(
      math.min(pt1.x, pt2.x), math.max(pt1.x, pt2.x),
      math.min(pt1.y, pt2.y), math.max(pt1.y, pt2.y)
)

    val tiffRF = spark.read.geotiff.loadRF(new java.net.URI(file))

    tiffRF.show(false)
    // tiffRF.where(st_intersects(tiffRF("bounds"),Extent(-85.32,41.27,-80.79,43.42))).show()
    // tiffRF.where(st_intersects("bounds",region)).show()
    tiffRF
    // .filter()                            // Use the filter/query API to
   .where(BOUNDS_COLUMN intersects pt1)
    // .result  
    
  }
}