import java.io.PrintWriter

import healpix.essentials._
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * ra(Right ascension):赤经 α
  * dec(Declination):赤纬 δ
  *
  * 极坐标
  * 倾斜角phi φ
  * 方位角theta θ
  * theta: Colatitude in radians [0,Pi]
  * phi:   Longitude in radians [0,2*Pi]
  * Place       Latitude  Colatitude
  * North pole  90°	    0°
  * Equator	    0°	      90°
  * South pole	−90°	    180°
  **/

var ra = 353.7877640000
var dec = -0.0950830000

val PI = Math.PI
val TWOPI = Constants.twopi

//var phi = ra * Constants.twopi / 360
var phi = Math.toRadians(ra)
//var theta = (90 - dec) * Math.PI / 180
var theta = Math.toRadians(90-dec)

val pointing = new Pointing(1.5724558382075704, 6.174761335068663)
val healpixBase = new HealpixBase(2, Scheme.NESTED)

println("HEALPix index:"+healpixBase.ang2pix(pointing))

def toPix(coordinate: (Double, Double)): (Long, (Double, Double)) = {
  ra = coordinate._1
  dec = coordinate._2
  phi = ra * TWOPI / 360
  theta = (90 - dec) * PI / 180
  (healpixBase.ang2pix(pointing), coordinate)
}


/*

val file = Source.fromFile(".\\res\\2MASS.txt")
val lineIterator = file.getLines()
val indics = lineIterator.map(s => {
  val coordinate = s.split(',')
  (coordinate(0).toDouble, coordinate(1).toDouble)
}).map(toPix)

indics.foreach(println)
*/

//val out=new PrintWriter("E:/output1.txt")
//out.write(indics.mkString)


