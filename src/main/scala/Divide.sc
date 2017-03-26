import java.io.{BufferedWriter, FileWriter, File}

import scala.io.Source

val inputFile = Source.fromFile("D:\\2MASS_OUTPUT.txt")

val lines = inputFile.getLines()

// 给一个order序列，生成id对应的order集合
def highOrder(id: Long, order: Int, highestOrder: Int = 15): Long = id >> (highestOrder - order) * 2

def lowOrder(id: Long, order: Int, highestOrder: Int = 15): Long = id >> (highestOrder - order) * 2

def toOrders(array: Array[Int], id: Long): Array[Long] = for (i <- array) yield highOrder(id, i)

// 去掉文本中的括号，再分割数据成(K,V)形式
def dataImporter(str: String): (Long, (Double, Double)) = {
  val array = str.init.tail.split(',')
  (array(0).toLong, (array(1).toDouble, array(2).toDouble))
}

def show(group: Iterator[_], num: Int = 5) = group.take(num).foreach(println)

val dataTuple = lines.map(dataImporter)
// 降序
val dataOrder = dataTuple.map(data => highOrder(data._1, 2) -> data._2)

// 根据orders构建路径
show(dataOrder)
dataTuple.take(2).foreach(v => println(toOrders(Array(0, 1, 2), v._1).mkString("", "\\", "\n")))


//生成文件
var fw: FileWriter = null
var file: File = null
var buffer: BufferedWriter = null
var flag = false
val highestOrder = 10
dataTuple
  //  .map(data => highOrder(data._1, highestOrder) -> data._2)
  .reduce((v1, v2) => {
  // 根据ID创建新文件
  if (!flag) {
    val location = toOrders(Array(0), v1._1).mkString("D:\\OUTPUT\\", "\\", "\\")
    file = new File(location)
    file.mkdirs()
    fw = new FileWriter(location + highOrder(v1._1, 5).toString + ".txt", true)
    buffer = new BufferedWriter(fw)
    buffer.append(v1.toString())
    buffer.newLine()
  }
  // 判断key是否相同
  if (v1._1 == v2._1) {
    buffer.append(v2.toString())
    buffer.newLine()
    flag = true
  }


  else {
    flag = false
    buffer.flush()
    buffer.close()
  }
  v2
})

/*
dataOrder.take(10).foreach(value => {
  buffer.append(value.toString()).append("\r\n")
})
buffer.close()
fw.close()*/

//val fw = new PrintWriter(new BufferedWriter(new FileWriter(filename)))