import java.io._

import healpix.essentials._

import scala.io.Source

/**
  * Created by Chen on 2016/11/15 0015.
  */
object PreProcess extends App {

  val location_2MASS = ".\\res\\2MASS.txt"
  val location_SDSS = ".\\res\\SDSS.txt"
  val output_2MASS = ".\\res\\2MASS_OUTPUT.txt"
  val output_SDSS = ".\\res\\SDSS_OUTPUT.txt"

  val file_2MASS = Source.fromFile(location_2MASS)
  val file_SDSS = Source.fromFile(location_SDSS)

  val records_2MASS = file_2MASS.getLines()
  val records_SDSS = file_SDSS.getLines()
  // HEALPix
  val N_side = 32768
  val healpixBase = new HealpixBase(N_side, Scheme.NESTED)
  var pointing = new Pointing()
  //val ra2phi = (ra: Double) => ra * Constants.twopi / 360
  //val dec2theta = (dec: Double) => (90 - dec) * Math.PI / 180
  // 赤经转弧度
  val ra2phi = (ra: Double) => Math.toRadians(ra)
  // 赤纬转弧度，[0,PI]
  val dec2theta = (dec: Double) => Math.toRadians(90 - dec)

  // 提取坐标，添加HEALPix索引
  def coord2pix(coordinate: Tuple2[String, String]): (Long, Double, Double) = {
    pointing.phi = ra2phi(coordinate._1.toDouble)
    pointing.theta = dec2theta(coordinate._2.toDouble)
    (healpixBase.ang2pix(pointing), pointing.phi, pointing.theta)
    //    (healpixBase.ang2pix(pointing), coordinate._1.toDouble, coordinate._2.toDouble)
  }

  // 文件输出格式
  def output(record: (Long, Double, Double), fs: FileWriter): Writer = {
    fs.append(record.toString).append("\r\n")
  }

  // 2MASS
  val fs_2MASS = new FileWriter(output_2MASS)
  val values_2MASS = {
    records_2MASS.map(_.split(','))
      .map(record => (record(0), record(1)))
      .map(coord2pix)
  }
  values_2MASS.foreach(output(_, fs_2MASS))

  fs_2MASS.close()

  // SDSS
  val fs_SDSS = new FileWriter(output_SDSS)
  val values_SDSS = {
    records_SDSS.map(_.split(','))
      .map(record => (record(1), record(2)))
      .map(coord2pix)
  }
  values_SDSS.foreach(output(_, fs_SDSS))
  fs_SDSS.close()
}
