/**
  * Created by msi- on 2016/12/6 0006.
  */

import java.lang.Math._

import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.runtime.RichInt

object CrossMatchSpark {
  def main(args: Array[String]): Unit = {
    val file2MASS = ".\\res\\2MASS_OUTPUT.txt"
    val fileSDSS = ".\\res\\SDSS_OUTPUT.txt"
    val output = ".\\res\\output"

    val conf = new SparkConf().setAppName("CrossMatch").setMaster("local").setSparkHome(System.getenv("SPARK_HOME"))
    val sc = new SparkContext(conf)
    val data2MASS = sc.textFile(file2MASS).map(dataImporter)
    val dataSDSS = sc.textFile(fileSDSS).map(dataImporter)
    // 格式：(HEALPix,((ra1,dec1),(ra2,dec2)))
    //    data2MASS.map((s: String) => s.indexOf(",")) ???

    //    val values_SDSS = dataSDSS.map(_.replace("(", "").replace(")", ""))
    //      .map(_.split(','))
    //      .map(array => array(0).toLong -> (array(1).toDouble, array(2).toDouble))
    //
    //    val joinData = data2MASS.map(_.replace("(", "").replace(")", ""))
    //      .map(_.split(','))
    //      .map(array => array(0).toLong -> (array(1).toDouble, array(2).toDouble))
    //      .join(values_SDSS).sortByKey() // Join

    //    val joinData = data2MASS.map(_.replace("(", "").replace(")", ""))
    val joinData = data2MASS.join(dataSDSS) // Join

    val result = joinData.map(data => (data._1, (crossMatcher(data._2._1, data._2._2), data._2))).filter(_._2._1 < 0.000034722)

//    println(s"SDSS ${dataSDSS.count}\n2MASS ${data2MASS.count()}\nJoinData ${joinData.count()}\nResult ${result.count()}")

//    result.saveAsTextFile(output)

    sc.stop()
  }

  // 去掉文本中的括号，再分割数据成(K,V)形式
  def dataImporter(str: String): (Long, (Double, Double)) = {
    val array = str.init.tail.split(',')
    (array(0).toLong, (array(1).toDouble, array(2).toDouble))
  }
  // 证认
  def crossMatcher(data1: (Double, Double), data2: (Double, Double), r: Double = 0.000034722): Double = {
    val ra1 = data1._1
    val dec1 = data1._2
    val ra2 = data2._1
    val dec2 = data2._2
//    sqrt(pow(toRadians(ra1 - ra2) * cos(toRadians((dec1 + dec2) / 2)), 2) + pow(toRadians(dec1 - dec2), 2))
    sqrt(pow((ra1 - ra2) * cos(toRadians((dec1 + dec2) / 2)), 2) + pow(dec1 - dec2, 2))
  }
}
/*
* SDSS 1082967
2MASS 34790
JoinData 1609031
Result 1609031
*/