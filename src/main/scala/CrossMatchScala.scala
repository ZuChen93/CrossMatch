import java.io.FileWriter

import healpix.essentials.FastMath
import scala.io.Source
/**
  * Created by Chen on 2016/11/15 0015.
  */
object CrossMatchScala extends App{
  val output_2MASS = ".\\res\\2MASS_OUTPUT.txt"
  val output_SDSS = ".\\res\\SDSS_OUTPUT.txt"
  val file_2MASS = Source.fromFile(output_2MASS)
  val file_SDSS = Source.fromFile(output_SDSS)
  val records_2MASS = file_2MASS.getLines()
  val records_SDSS = file_SDSS.getLines()
  val file = new FileWriter("E:\\CrossMatch.txt")

  val replaceSequence: CharSequence = "()"
  val values_SDSS = records_SDSS.map(_.replace("(", "").replace(")", ""))
    .map(_.split(','))
    .map(array => (array(0).toLong, array(1).toDouble, array(2).toDouble))

  records_2MASS.map(_.replace("(", "").replace(")", ""))
    .map(_.split(','))
    .map(array => (array(0).toLong, array(1).toDouble, array(2).toDouble))
    .foreach({
      star_2MASS => {
        file.append(s"== $star_2MASS").append("\r\n")
        values_SDSS.filter(_._1.equals(star_2MASS._1)).map(star_SDSS => crossMatcher(star_SDSS._2, star_2MASS._2, star_SDSS._3, star_2MASS._3))
          .foreach(record => file.append(record.toString()).append("\r\n"))
      }
    })

  file.close()

//  val dec2theta = (dec: Double) => (90 - dec) * Math.PI / 180

  // TODO:R取多少？
  // cos参数为弧度
  // println(FastMath.cos(Constants.twopi))
  def crossMatcher(ra1: Double, dec1: Double, ra2: Double, dec2: Double, r1: Double = 1, r2: Double = 1) = {
//    Math.sqrt(Math.pow((ra1 - ra2) * Math.cos((90 - dec) * Math.PI / 180), 2) + Math.pow(dec1 - dec2, 2))
    Math.sqrt(Math.pow(Math.toRadians(ra1 - ra2) * Math.cos(Math.toRadians((dec1 + dec2) / 2)), 2) + Math.pow(Math.toRadians(dec1 - dec2), 2))
  }
}

