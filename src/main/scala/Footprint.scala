import java.io.File
import java.lang.Math.{cos, pow, sqrt, toRadians}

import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

import scala.math.LowPriorityOrderingImplicits


/**
  * Created by Chen on 2017/3/5 0005.
  */
object Footprint {
  def main(args: Array[String]): Unit = {
    val file2MASS = ".\\res\\2MASS_OUTPUT.txt"
    val fileSDSS = ".\\res\\SDSS_OUTPUT.txt"
    //    val output = ".\\res\\output.txt"
    val output = "D:\\footprint"

    //    deleteFile(new File(output))


    val N_side = 15
    val conf = new SparkConf().setAppName("CrossMatch").setMaster("local").setSparkHome(System.getenv("SPARK_HOME"))
    val sc = new SparkContext(conf)

    val data2MASS = sc.textFile(file2MASS).map(dataImporter)
    val dataSDSS = sc.textFile(fileSDSS).map(dataImporter)


    val twoOrderData = data2MASS.map {
      case (healpixId, coordinateTuple) => (twoOrder(healpixId, 2), coordinateTuple)
    }
    val distinctTwoOrderData = twoOrderData.reduceByKey((v1, v2) => v2)
    //    var highOrderSingleData = distinctTwoOrderData.map { case ((highOrder, lowOrder), coordinate) => highOrder -> lowOrder }
    //      .combineByKey[List[Long]](
    //      (list: Long) => List(list),
    //      (list: List[Long], lowOrder: Long) => list ++ List(lowOrder),
    //      (v1: List[Long], v2: List[Long]) => v1 ++ v2
    //    )
    // 另一种方法
    val highOrderSingleData = distinctTwoOrderData.map { case ((highOrder, lowOrder), coordinate) => highOrder -> lowOrder }
      .aggregateByKey(Nil: List[Long])((list: List[Long], lowOrder: Long) => list ++ List(lowOrder), (v1, v2) => v1 ++ v2)


    val pickupDataFilter = highOrderSingleData.filter(_._2.size < 4).map(_._1)
    val keepDataFilter = highOrderSingleData.filter(_._2.size == 4).map(_._1)
    val mask = sc.broadcast(pickupDataFilter.collect)
    twoOrderData.filter(v => mask.value.contains(v._1._1)).foreach(println)

    // ID -> 1, 仿照wordcount计数
    // dataSDSS.map(pixel => (highOrder(pixel._1, 14), 1)).reduceByKey(_ + _).filter(v => v._2 > 2).saveAsTextFile(output)
    // dataSDSS.map(pixel => (highOrder(pixel._1, 14), 1)).filter(_._1==1231657334).foreach(v=>println(v.toString()))
    // group方法
    // dataSDSS.map(pixel => (twoOrder(pixel._1, 15), pixel._2)).groupBy(v => v._1._1).filter(v => v._2.size > 3).foreach(v => println(v))
    // 测试

    //    val order = dataSDSS.map { case (healpixID, (dec, ra)) => (highOrder(healpixID, 14), (lowOrder(healpixID, 14), healpixID, dec, ra)) }
    //    )
    //    val createCombiner = {
    //      case (highOrder, lowOrder, healpixID, de, ra) => highOrder
    //    }
    //    val combine = order.combineByKey(
    //      (v: (Long, Long, Double, Double)) => new List(v._1) -> new List(v._2, v._3, v._4),
    //      (value: C, tuple: (Long, Long, Double, Double)) =>

    var orders = List[Int]()
    val array = sc.parallelize((1 to 24).toList ++ List(2, 3, 4, 5, 6)).cache

    val cache = array.map(v => (twoOrder(v, 1, 2), v))
      .map { case ((high, low), id) => ((high, low), List(id)) }
      .reduceByKey(_ ++ _)
      .map { case ((high, low), id) => (high, (List(low), id)) }
      .reduceByKey((v1, v2) => (v1._1 ++ v2._1, v1._2 ++ v2._2))
      .flatMap { case (high, (lowList, idList)) =>
        if (lowList.size == 4)
          idList.map(id => ((high, 1), id))
        else
          idList.map(id => ((id, 2), id))
      }.filter { case ((_, order), _) => order != 2 }

    // TODO: 循环？？
    // TODO: treeReduce怎么用???
    // TODO: footprint重合
    /*    println("Orders:" + orders.mkString(" "))
        cache.filter { case ((_, low), _) => low == 2 }.map(item => item._2).foreach(println)*/


    //        cache.map(v => {1
    //          if (v._2.size > 3)
    //            for (item <- v._2)
    //              yield item
    //        }).foreach(println)
    sc.stop()
  }

  // 递归删除文件夹
  // TODO: 改成object类
  def deleteFile(file: File) {
    if (file.exists()) {
      //判断文件是否存在
      if (file.isFile) {
        //判断是否是文件
        file.delete(); //删除文件
      } else if (file.isDirectory) {
        //否则如果它是一个目录
        val files = file.listFiles(); //声明目录下所有的文件 files[];
        for (f <- files) {
          //遍历目录下所有的文件
          f.delete() //把每个文件用这个方法进行迭代
        }
        file.delete(); //删除文件夹
      }
    } else {
      System.out.println("所删除的文件不存在");
    }
  }

  // 去掉文本中的括号，再分割数据成(K,V)形式
  def dataImporter(str: String): (Long, (Double, Double)) = {
    val array = str.init.tail.split(',')
    (array(0).toLong, (array(1).toDouble, array(2).toDouble))
  }


  // 给一个order序列，生成id对应的order集合
  def highOrder(id: Long, order: Int, highestOrder: Int = 15): Long = id >> (highestOrder - order) * 2

  //  def lowOrder(id: Long, order: Int, highestOrder: Int = 15): Long = (id >> (highestOrder - order) * 2) & 0x3
  def lowOrder(id: Long, order: Int, highestOrder: Int = 15): Long = (id >> (highestOrder - order - 1) * 2) & 0x3

  def twoOrder(id: Long, order: Int, highestOrder: Int = 15): (Long, Long) = {
    // val highOrder = id >> (highestOrder - order) * 2
    (id >> (highestOrder - order) * 2, id >> (highestOrder - order - 1) * 2 & 0x3)
  }

  def toOrders(array: Array[Int], id: Long): Array[Long] = for (i <- array) yield highOrder(id, i)


  class HealpixPartitioner(partitions: Long, k_Healpix: Int) extends Partitioner {
    override def numPartitions = partitions.toInt

    override def getPartition(key: Any): Int = key match {
      case k: Long => (k >> k_Healpix * 2).toInt
    }
  }

}


/*    /* 网上找的例子
    * 原始数据：l1
    * 结果：
    * ("To", RDD(("Tom",120),("Tod","70"))
    * ("Ja", RDD(("Jack",120),("James","55"),("Jane",15))
    * */
    val l1 = List(("To", List(("Tom", 50), ("Tod", 30), ("Tom", 70), ("Tod", 25), ("Tod", 15))),
      ("Ja", List(("Jack", 50), ("James", 30), ("Jane", 70), ("James", 25), ("Jasper", 15))))
    sc.parallelize(l1).flatMap { case (key, list) => list.map(item => ((key, item._1), item._2)) }
      .reduceByKey(_ + _)
      .map { case ((key, name), hours) => (key, List((name, hours))) }
      .reduceByKey(_ ++ _)*/