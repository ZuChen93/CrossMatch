val array = (1 to 24).toList ++ List(2, 3, 4, 5, 6)
val cache = array.map(v => (twoOrder(v, 1, 2), v))

// 给一个order序列，生成id对应的order集合
def highOrder(id: Long, order: Int, highestOrder: Int = 15): Long = id >> (highestOrder - order) * 2

def lowOrder(id: Long, order: Int, highestOrder: Int = 15): Long = (id >> (highestOrder - order) * 2) & 0x3

def twoOrder(id: Long, order: Int, highestOrder: Int = 15): (Long, Long) = {
  // val highOrder = id >> (highestOrder - order) * 2
  (id >> (highestOrder - order) * 2, id >> (highestOrder - order - 1) * 2 & 0x3)
}

def toOrders(array: Array[Int], id: Long): Array[Long] = for (i <- array) yield highOrder(id, i)
//        cache.map(v => {
//          if (v._2.size > 3)
//            for (item <- v._2)
//              yield item
//        }).foreach(println)