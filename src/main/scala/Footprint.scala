import java.io.File

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * Created by Chen on 2017/3/5 0005.
  */
object Footprint {

  val N_side: Int = 15

  def main(args: Array[String]): Unit = {
    val file2MASS = ".\\res\\2MASS_OUTPUT.txt"
    val fileSDSS = ".\\res\\SDSS_OUTPUT.txt"
    // val output = ".\\res\\output.txt"
    val output = "D:\\footprint"

    // deleteFile(new File(output))

    val conf = new SparkConf().setAppName("CrossMatch").setMaster("spark://ip:7077").setSparkHome(System.getenv("SPARK_HOME"))
    val sc = new SparkContext(conf)

    //    val data2MASS = sc.textFile(file2MASS).map(dataImporter)
    //    val dataSDSS = sc.textFile(fileSDSS).map(dataImporter)

    var firstRound = sc.broadcast(true)

    var keepData: Broadcast[Array[((Long, Byte), (Double, Double))]] = null

    for (i <- 13 to 14) {
      var twoOrderData: RDD[((Long, Byte), (Double, Double))] = null

      if (firstRound.value) {
        // 把读入文件的过程应该放进循环里比较好吧？
        val data2MASS = sc.textFile(file2MASS).map(dataImporter)
        /*
        * HealPix分解成 上一级高位High+本级低位Low 形式
        * ((281,1),(0.3601450701050618,1.5874289965420452))
        * ((280,3),(0.18082391104499088,1.58617386963976))
        * ((278,2),(0.47050725285909084,1.5789050249842687))
        * ((281,1),(0.32655967199906594,1.579745990452854))
        * ((292,3),(6.071633457316581,1.5865394497965875))
        * */
        twoOrderData = data2MASS.map {
          case (healpixId, coordinateTuple) => (twoOrder(healpixId, i), coordinateTuple)
        }
        firstRound = sc.broadcast(false)
        println("first")
      }
      else {
        println("next")
        twoOrderData = sc.makeRDD(keepData.value).map {
          case ((highOrder, _), coordinateTuple) => (twoOrder(highOrder, i), coordinateTuple)
        }
      }

      /*
    * 去重，方便后面统计低位个数（00、01、10、11）
    * todo:直接对healpix去重不得了？？？
    * ((278,2),(0.47050725285909084,1.5789050249842687))
    * ((281,1),(0.32655967199906594,1.579745990452854))
    * ((292,3),(6.071633457316581,1.5865394497965875))
    * */
      val distinctTwoOrderData = twoOrderData.reduceByKey((v1, v2) => v2)
      /*
    * 同一HighOrder的LowOrder归类
    * (74,List(1))
    * (90,List(0, 2))
    * (72,List(3))
    * (70,List(1, 0, 2))
    * (68,List(3))
    * (73,List(1, 3, 2, 0))
    * (75,List(0))
    * (143,List(3))
    * (69,List(0, 1, 2))
    * (67,List(3))
    * */
      val highOrderSingleData = distinctTwoOrderData.map { case ((highOrder, lowOrder), coordinate) => highOrder -> lowOrder }
        // 转成Byte方便循环时的模式匹配，不知道会不会降低性能
        .combineByKey[List[Byte]](
        (list: Byte) => List(list),
        (list: List[Byte], lowOrder: Byte) => list ++ List(lowOrder),
        (v1: List[Byte], v2: List[Byte]) => v1 ++ v2
      )
      // 另一种方法，用aggregateByKey实现
      //val highOrderSingleData = distinctTwoOrderData.map { case ((highOrder, lowOrder), coordinate) => highOrder -> lowOrder }
      //  .aggregateByKey(Nil: List[Long])((list: List[Long], lowOrder: Long) => list ++ List(lowOrder), (v1, v2) => v1 ++ v2)

      // 筛选出满足条件的High分开，作为过滤器，分别对原数据集过滤
      /**
        * 此方法不妥，collect操作会将数据传回driver，数据量大的时候可能有性能损耗，
        * 似乎只要不以RDD形式存在，数据就会传回到driver内存中。
        *
        * @note this method should only be used if the resulting array is expected to be small, as
        *       all the data is loaded into the driver's memory.
        *       但是不用collect，filter还是RDD形式存在，没有像Array一样的contains方法
        **/
      val pickupDataFilter = highOrderSingleData.filter(_._2.size < 4).map(_._1)
      val keepDataFilter = highOrderSingleData.filter(_._2.size == 4).map(_._1)
      // 直接调用过滤器变量会报错，只能用广播的方法再把过滤器变量广播到所有worker上
      val bcPickupDataFilter = sc.broadcast(pickupDataFilter.collect)
      val bcKeepDataFilter = sc.broadcast(keepDataFilter.collect)
      // 不向上聚合的数据
      val pickupData = twoOrderData.filter(v => bcPickupDataFilter.value.contains(v._1._1))
      // TODO: 直接存到hdfs中？一堆文件合不合适？
      pickupData.saveAsTextFile(output + "\\" + i)
      // TODO: 或者collect传回driver？费不费内存？
      // val tempData=pickupData.collect()
      // 继续聚合的数据
      keepData = sc.broadcast(twoOrderData.filter(v => bcKeepDataFilter.value.contains(v._1._1)).collect)
    }

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

    /*
     *  构建简单数据测试
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
    */

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
  def highOrder(id: Long, order: Int, highestOrder: Int = N_side): Long = id >> (highestOrder - order) * 2

  //  def lowOrder(id: Long, order: Int, highestOrder: Int = N_side): Long = (id >> (highestOrder - order) * 2) & 0x3
  def lowOrder(id: Long, order: Int, highestOrder: Int = N_side): Long = (id >> (highestOrder - order - 1) * 2) & 0x3

  def twoOrder(id: Long, order: Int, highestOrder: Int = N_side): (Long, Byte) = {
    // val highOrder = id >> (highestOrder - order) * 2
    (id >> (highestOrder - order) * 2, (id >> (highestOrder - order - 1) * 2 & 0x3).toByte)
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