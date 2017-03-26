/**
  * Created by msi- on 2016/12/6 0006.
  */

import java.io.{BufferedWriter, FileWriter}
import java.lang.Math._
import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import org.apache.spark._
import sun.awt.image.ImageCache.PixelsKey

import scala.collection.mutable.ArrayBuffer


object CrossMatchPartition {


  def main(args: Array[String]): Unit = {
    val file2MASS = ".\\res\\2MASS_OUTPUT.txt"
    val fileSDSS = ".\\res\\SDSS_OUTPUT.txt"
    val output = ".\\res\\output"

    val N_side = 32768
    //    val k = 15
    val conf = new SparkConf().setAppName("CrossMatch").setMaster("local").setSparkHome(System.getenv("SPARK_HOME"))
    val sc = new SparkContext(conf)

    val data2MASS = sc.textFile(file2MASS).map(dataImporter)
    val dataSDSS = sc.textFile(fileSDSS).map(dataImporter).distinct()
    // data2MASS.saveAsTextFile("partition")
    // 去重
    val data2MASS_reduce = data2MASS.map(pixel=>highOrder(15,2,pixel._1)->pixel._2)
    val num=data2MASS.partitionBy(new HealpixPartitioner(data2MASS_reduce.reduceByKey((t1,_)=>t1).count,2)).saveAsTextFile("fuck")
//    data2MASS_reduce.reduceByKey((t1,_)=>t1).take(10).foreach(println)
    println(num)
/*    println(s"Distinct before:${data2MASS.count}\tDistinct after:${data2MASS_reduce.count()}")
    val divided_data2MASS = data2MASS_reduce.partitionBy(new HealpixPartitioner(data2MASS_reduce.reduceByKey((t1, _) => t1).count(), 0))
    println(s"data2MASS partitions:${data2MASS.getNumPartitions}\t partitions:${divided_data2MASS.getNumPartitions}")
   divided_data2MASS.glom().saveAsTextFile("glom")*/

/*    val high = data2MASS.map((pixel: (Long, (Double, Double))) => highOrder(15, 0, pixel._1) -> pixel._2)
    val multiOrders = data2MASS.map(pixel => toOrders(Array(0, 5, 10, 15), pixel._1))
    multiOrders.takeSample(false, 10).foreach(sample => {
      println(sample.mkString("→"))
    })*/

    println(s"Distinct before:${data2MASS.count()}")
    //    println(s"Partitions:${high.getNumPartitions}")
    //    println(s"Order:${high.distinct.count}")

    //    val joinData = data2MASS.join(dataSDSS) // Join
    //
    //    val result = joinData.map(data => (data._1, (crossMatcher(data._2._1, data._2._2), data._2))).filter(_._2._1 < 0.000034722)

    //    println(s"SDSS ${dataSDSS.count}\n2MASS ${data2MASS.count()}\nJoinData ${joinData.count()}\nResult ${result.count()}")

    //        result.saveAsTextFile("output")


    //    toOrders(Array(9,10),4988168485L)
//    val fw=new FileWriter("D:\\OUTPUT.txt")
//    val buffer=new BufferedWriter(fw)
//    dataOrder.take(10).foreach(value => {
//      buffer.append(value.toString()).append("\r\n")
//    })
//    buffer.close()
//    fw.close()
//    sc.stop()
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

    //    val partitioner=new Partitioner {override def numPartitions = 12
    //
    //      override def getPartition(key: Any) =key.toString.toLong
    //    }
  }

  def OrganizeFiles(hierarchy: Array[Int]): Unit = {
    //    hierarchy.foreach(hierarchy.length)
  }

  // 给一个order序列，生成id对应的order集合
  def highOrder(highestOrder: Int, order: Int, id: Long): Long = id >> (highestOrder - order) * 2

  def lowOrder(highestOrder: Int, order: Int, id: Long): Long = id >> (highestOrder - order) * 2

  def toOrders(array: Array[Int], id: Long): Array[Long] = for (i <- array) yield highOrder(15, i, id)

  // footprint

  class HealpixPartitioner(partitions: Long, k_Healpix: Int) extends Partitioner {
    override def numPartitions = partitions.toInt

    override def getPartition(key: Any): Int = key match {
      case k: Long => (k >> k_Healpix * 2).toInt
    }
  }

}

/*
* SDSS 1082967
2MASS 34790
JoinData 1609031
Result 1609031
*/