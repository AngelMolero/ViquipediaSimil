package mapreduce

object InfoFicheros {
  var ficheros = List[String]()
  var numDocumentos = 0
  var idf = Map[String, Double]()
  var tfidf = Map[(String, String), List[Double]]()
  var fitxConten =  Map[String, List[String]]()
  var fitxRefs =  Map[String, List[String]]()
  var fitxTratar = List[(String, List[String])]()
  var fitxComb = Map[(String,String), List[String]]()
  var palabrasCont = List[((String, String), List[Int])]()
  var tituloTFIDF = Map[String, List[(String,Double)]]()
  var combinaciones = Map[String, List[String]]()
  var raizSumatorio = Map[String, Double]()

  def existRef(titulo1: String, titulo2: String): Boolean = {
    !fitxRefs(titulo1).contains("[["++titulo2++"]]")
  }
  def groupBy3(list1: List[(String, Double)], list2: List[(String, Double)]): Map[String, Double] = {
    list1.foldLeft(list2.toMap) {
      case (acc, (key, value)) => acc.get(key) match {
        case None =>
          acc + (key -> 0.0)
        case Some(oldValue) =>
          acc + (key -> (value * oldValue))
      }
    }
  }

  def vectorAlignment(v1: List[(String, Double)], v2: List[(String, Double)]): Double = {
    if (v1.isEmpty || v2.isEmpty) 0
    else if (v1.head._1 == v2.head._1) v1.head._2 * v2.head._2 + vectorAlignment(v1.tail, v2.tail)
    else vectorAlignment(v1.tail, v2)
  }

  def cosinesim(doc1: List[(String, Double)], denomin: Double, i: String): Double = {
    val doc2 = InfoFicheros.tituloTFIDF(i)
   // val p = if (doc1.length > doc2.length) groupBy3(doc1, doc2) else groupBy3(doc2, doc1)
   // val numerador = p.values.sum
    val numerador = vectorAlignment(doc1, doc2)
    //val p = agrupacion(doc1 ::: doc2)(_._1).map(a => (a._1, a._2.padTo(2, ("", 0.0))))
    //val numerador = p.map(a => (a._1, a._2.padTo(2, ("", 0.0)).foldLeft(1.0)((x, y) => x * y._2))).foldLeft(0.0)((x, y) => x + y._2)
    val denominador = denomin * InfoFicheros.raizSumatorio(i)
    numerador / denominador
  }

  /*def cosinesim(doc1: List[(String, Double)], denomin: Double, i: String): Double ={
    val doc2 = InfoFicheros.tituloTFIDF(i)
    val p = (doc1 ::: doc2).groupBy(_._1)
    //val p = agrupacion(doc1 ::: doc2)(_._1).map(a => (a._1, a._2.padTo(2, ("", 0.0))))
    val numerador = p.map(a => (a._1, a._2.padTo(2, ("", 0.0)).foldLeft(1.0)((x, y) => x * y._2))).foldLeft(0.0)((x, y) => x + y._2)
    val denominador = denomin * InfoFicheros.raizSumatorio(i)
    numerador / denominador
  }*/
}
