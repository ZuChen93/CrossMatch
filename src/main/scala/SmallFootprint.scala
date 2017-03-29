import java.io.File

import org.apache.spark._
import org.apache.spark.rdd.RDD

/**
  * Created by Chen on 2017/3/5 0005.
  */
object SmallFootprint {

  val N_side: Int = 2

  def main(args: Array[String]): Unit = {
    val fileSmallData = "./res/smallData.txt"

    val output = "./res/footprint/"


    deleteFile(new File(output))

    val conf = new SparkConf().setAppName("CrossMatch").setMaster("local").setSparkHome(System.getenv("SPARK_HOME"))
    val sc = new SparkContext(conf)

    val smallData = sc.textFile(fileSmallData).map(dataImporter)

    //    val data2MASS = sc.textFile(file2MASS).map(dataImporter)
    //    val dataSDSS = sc.textFile(fileSDSS).map(dataImporter)

    var firstRound = sc.broadcast(true)

    // 反复操作的数据
    var iterativeData = smallData.map {
      case (healpixId, (ra, dec)) => (healpixId, (false, (healpixId, ra, dec)))
    }
    // Boolean如果为true证明可以继续聚合，false表示不再聚合
    var tempData: RDD[((Long, Int), (Boolean, (Long, Double, Double)))] = null

    for (i <- Array(1, 0)) {
      // 分出高低order
      val twoOrderData = iterativeData.map {
        case (healpixId, others) => (twoOrder(healpixId, i), others)
      }
      // 对(highOrder, lowOrder)去重
      // TODO: 还不行，还得换回来。。
      val distinctData = twoOrderData.reduceByKey((v1, _) => v1)

      // 将相同highOrder的lowOrders放在一起
      val highOrderSingleData = distinctData.map { case ((highOrder, lowOrder), _) => highOrder -> lowOrder }
        .combineByKey[List[Byte]](
        (list: Byte) => List(list),
        (lowOrder: List[Byte], list: Byte) => lowOrder ++ List(list),
        (v1: List[Byte], v2: List[Byte]) => v1 ++ v2
      )
      // 根据需要分成保留组和筛出组
      val pickupDataFilter = highOrderSingleData.filter(_._2.size < 4)
      val keepDataFilter = highOrderSingleData.filter(_._2.size == 4)
      // 将highOrder作为key，去除lowOrder信息（应该没用了），其他信息作为value
      val highOrderData = twoOrderData.map {
        case ((highOrder, _), (aggregateOrNot, otherInfo)) => highOrder -> (aggregateOrNot, otherInfo)
      }

      val pickupData = highOrderData.subtractByKey(keepDataFilter).map(v => ((v._1, i + 1), v._2))
      val keepData = highOrderData.subtractByKey(pickupDataFilter).map(v => ((v._1, i + 1), v._2))
      tempData = keepData


      // 当前选出的数据还不能直接拿出去，还得和下一次比对
      pickupData.foreach(println)
      println("----------")
      keepData.foreach(println)
      println("==========")


      //      highOrderData.subtractByKey(keepDataFilter).map(v => ((v._1, i + 1), v._2)).saveAsTextFile(output + (i + 1))
      //
      //      iterativeData = highOrderData.subtractByKey(pickupDataFilter) // 用subtractByKey可以直接筛出去
      //
      if (i == 0)
        {
          keepData.foreach(println)
          println(keepData.count)
        }
      //              iterativeData.map(v => ((v._1, i), v._2)).saveAsTextFile(output + i)

    }
    /**
      * @note 突然想明白了，Hadoop里面就是不同的把数据转换成k，v形式处理，那么实际MapReduce就是不断的处理变换k，v再处理的过程啊！
      */
    //    sc.wholeTextFiles(output + "*/part-00000*").repartition(1).saveAsTextFile(output + "final")

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
      System.out.println("所删除的文件不存在")
    }
  }

  // 去掉文本中的括号，再分割数据成(K,V)形式
  def dataImporter(str: String): (Long, (Double, Double)) = {
    val array = str.init.tail.split(',')
    (array(0).toLong, (array(1).toDouble, array(2).toDouble))
  }


  // 给一个order序列，生成id对应的order集合
  def highOrder(id: Long, order: Int, highestOrder: Int = N_side): Long = id >> (highestOrder - order) * 2

  //  def lowOrder(id: Long, order: Int, highestOrder: Int = N_side): Long = (id >> (highestOrder - order) * 2) & 0x3
  def lowOrder(id: Long, order: Int, highestOrder: Int = N_side): Long = (id >> (highestOrder - order - 1) * 2) & 0x3

  def twoOrder(id: Long, order: Int, highestOrder: Int = N_side): (Long, Byte) = {
    // val highOrder = id >> (highestOrder - order) * 2
    (id >> (highestOrder - order) * 2, (id >> (highestOrder - order - 1) * 2 & 0x3).toByte)
  }

  def toOrders(array: Array[Int], id: Long): Array[Long] = for (i <- array) yield highOrder(id, i)


  class HealpixPartitioner(partitions: Long, k_Healpix: Int) extends Partitioner {
    override def numPartitions: Int = partitions.toInt

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