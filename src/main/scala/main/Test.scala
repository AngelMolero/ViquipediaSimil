package main

object Test {

  var n = List(("a", 1.0), ("aaa", 2.0), ("b", 3.0), ("bb", 4.0), ("c", 5.0), ("cc", 5.0))
  var m = List(("a", 1.0), ("aa", 2.0), ("aaa", 2.0), ("b", 3.0), ("bb", 4.0), ("c", 5.0))

  /*def vectorAlignment(v1: List[(String, Double)], v2: List[(String, Double)]): Double = {
    if (v1.isEmpty || v2.isEmpty) 0
    else if (v1.head._1 == v2.head._1) v1.head._2 * v2.head._2 + vectorAlignment(v1.tail, v2.tail)
    else vectorAlignment(v1.tail, v2)
  }*/

  def vectorAlignment(v1: List[(String, Double)], v2: List[(String, Double)]): Double = {
    // v1 > v2
    if (v1.isEmpty || v2.isEmpty) return 0
    val comp = v1.head._1.compare(v2.head._1)
    if(comp == 0) return v1.head._2 * v2.head._2 + vectorAlignment(v1.tail, v2.tail)
    else if(comp < 0) return vectorAlignment(v1.tail, v2)
    vectorAlignment(v1, v2.tail)
  }

  def main(args: Array[String]): Unit = {

    // mes curt esquerre
    println(m.drop(2))
  }
}
