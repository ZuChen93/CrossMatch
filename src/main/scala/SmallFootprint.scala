import java.io.File

import org.apache.spark._

/**
  * Created by Chen on 2017/3/5 0005.
  */
object SmallFootprint {
  /** 参数调整 */
  val N_side: Int = 2
  val aggregatedProvider = Array(0, 1)
  val fileSmallData = "./res/smallData.txt"
  val output = "./res/footprint/"

  def main(args: Array[String]): Unit = {

    // TODO:能不能用DataFrame优化数据结构？http://blog.csdn.net/lw_ghy/article/details/51480358

    deleteFile(new File(output))

    val conf = new SparkConf().setAppName("CrossMatch").setMaster("local").setSparkHome(System.getenv("SPARK_HOME"))
    val sc = new SparkContext(conf)


    /** @return 标上ID（区别于HealPixID），替换掉多余的坐标信息，减少内存占用
      * @note 两种编号方法：zipWithIndex全局有序（费时），zipWithUniqueId全局无序（只保证ID唯一）
      *       smallData.zipWithUniqueId().saveAsTextFile(output + "2") */
    val smallData = sc.textFile(fileSmallData).map(dataImporter).zipWithUniqueId()

    /** @return 反复操作的数据，开始就把id转化为List格式，方便后面迭代
      *         (healpixId, (N_side, List(id)) */
    var iterativeData = smallData.map {
      case ((healpixId, coordinate), id) => healpixId -> (N_side, List(id))
    }

    for (i <- aggregatedProvider.reverse) {
      /** @return (highOrder, (lowOrder, currentHighestOrder, id)) */
      val twoOrderData = iterativeData.map {
        // TODO: 因为每一次迭代中的highestOrder都不一样，所以需要每次变化
        case (healpixId, (currentHighestOrder, id)) =>
          highOrder(healpixId, i, currentHighestOrder) -> (currentHighestOrder, lowOrder(healpixId, i, currentHighestOrder), id)
      }

      /** @note 核心！此结果只是统计每个lowOrder个数，考虑多层List嵌套会浪费空间（一个大List里面有四个小List）
        *       没有将对应的HEALPixID归类，只是聚成有一个集合，如果后期聚合可以根据lowOrder重进计算筛选
        *       createCombiner操作：创建一个4元素数组，根据lowOrder对应的位置初始单位1；保留List(ID)格式和currentHighestOrder
        *       mergeValue操作：    两个数组对应元素相加；List(ID)相加；currentHighestOrder不变
        *       mergeCombiners操作：合并结果
        * @return 最后生成lowOrderArray，表示当前highOrder下lowOrder的分布，后面跟上同级的所有ID
        * @example (0, List(8, 4, 3, 4), List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 49, 50, 51, 52, 53, 54, 55, 57, 58))
        *          (4, List(4, 2, 0, 0), List(43, 44, 45, 46, 47, 48)) */
      val combineLowOrder = twoOrderData.combineByKey[(Int, Array[Int], List[Long])](
        (c: (Int, Byte, List[Long])) => {
          val array = Array(0, 0, 0, 0)
          array(c._2) = 1
          (c._1, array, c._3)
        },
        (c: (Int, Array[Int], List[Long]), value: (Int, Byte, List[Long])) => {
          c._2(value._2) += 1
          (c._1, c._2, c._3 ++ value._3)
        },
        (v0: (Int, Array[Int], List[Long]), v1: (Int, Array[Int], List[Long])) => (v0._1, addArray(v0._2, v1._2), v0._3 ++ v1._3))

      /** @return 根据需要分成保留组和筛出组，通过判断lowOrderArray里有没有0实现
        * @note   另一种方法直接分两组，看doc好像比较耗资源
        *         combineLowOrder.groupBy(_._2._1.contains(0)) */
      val pickupData = combineLowOrder.filter(_._2._2.exists(_ == 0))
      val keepData = combineLowOrder.filter(!_._2._2.contains(0))


      /** @return 将highOrder作为key，去除lowOrderArray信息（应该没用了），
        *         将currentHighestOrder替换成当前i值，其他信息作为value，传给下次迭代 */
      iterativeData = keepData.map {
        case (highOrder, (_, _, idList)) => (highOrder, (i, idList))
      }

      /** @note 一种统计List总数的方法：aggregate */
      println(s"第${i + 1}级的数据，导出${pickupData.aggregate(0)((count, tuple) => count + tuple._2._3.size, _ + _)}个")
      pickupData.map(tuple3 => (tuple3._1, tuple3._2._1, tuple3._2._2.toList, tuple3._2._3)).foreach(println)
      // println(s"可以聚合到下一级的数据${keepData.map(_._2._3.size).sum()}个")
      // keepData.map(tuple3 => (tuple3._1, tuple3._2._1, tuple3._2._2.toList, tuple3._2._3)).foreach(println)
      // println("=-=-=-=-=-=")

      // 最后一次迭代还要保存留下的数据
      if (i == 0) {
        /** 另一种统计List总数的方法：map 搭配 sum */
        println(s"第${i}级的数据，导出${keepData.map(_._2._3.size).sum()}个")
        keepData.map(tuple3 => (tuple3._1, tuple3._2._1, tuple3._2._2.toList, tuple3._2._3)).foreach(println)
      }
    }

    // 最后，重新取回所有生成的结果，合并成一个文件保存
    // sc.wholeTextFiles(output + "*/part-00000*").repartition(1).saveAsTextFile(output + "final")

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

  /** @param array0 第一个数组
    * @param array1 第二个数组
    * @return 两个数组每个对应元素相加的结果
    * @note 似乎数据的个数超不多Int的最大值 ，暂时用Int类型节省空间 */
  def addArray(array0: Array[Int], array1: Array[Int]): Array[Int] = {
    array0.map(v => v + array1(array0.indexOf(v)))
  }

  /** @return 高位 */
  def highOrder(id: Long, order: Int, highestOrder: Int = N_side): Long = {
    id >> (highestOrder - order) * 2
  }

  /** @return 低两位(00,01,10,11) */
  def lowOrder(id: Long, order: Int, highestOrder: Int = N_side): Byte = {
    ((id >> (highestOrder - order - 1) * 2) & 0x3).toByte
  }

  /** @return (高位,低位) */
  def twoOrder(id: Long, order: Int, highestOrder: Int = N_side): (Long, Byte) = {
    (id >> (highestOrder - order) * 2, (id >> (highestOrder - order - 1) * 2 & 0x3).toByte)
  }

  /** @return 返回给定array序列中的每一层号 */
  def toOrders(array: Array[Int], id: Long): Array[Long] = {
    for (i <- array) yield highOrder(id, i)
  }

  // 仿照 org.apache.spark..Partitioner.HashPartitioner 编写
  class HealpixPartitioner(partitions: Int, order: Int, highestOrder: Int = N_side) extends Partitioner {
    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = key match {
      // TODO:直接用分区数分区行么？用不用hashCode？
      case id: Long => highOrder(id, order, highestOrder).toInt % numPartitions
      case null => 0
    }

    /*    public int getPartition(K2 key, V2 value, int numReduceTasks) {
          return (key.hashCode() & 2147483647) % numReduceTasks;
        }*/
  }

}
