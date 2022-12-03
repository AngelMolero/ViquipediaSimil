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

  def groupBy2[K,V](collection: Seq[V])(keyFn: V => K): Map[K, Seq[V]] = {
    collection.foldLeft(Map.empty[K, Seq[V]]) {
      case (acc, value) =>
        val key = keyFn(value)
        acc.updated(key, acc.getOrElse(key, Seq.empty[V]) :+ value)
    }
  }

  def vectorAlignment(v1: List[(String, Double)], v2: List[(String, Double)]): Double = {
    // v1 > v2
    if (v1.isEmpty || v2.isEmpty) return 0
    val comp = v1.head._1.compare(v2.head._1)
    if(comp == 0) return v1.head._2 * v2.head._2 + vectorAlignment(v1.tail, v2.tail)
    else if(comp < 0) return vectorAlignment(v1.tail, v2)
    vectorAlignment(v1, v2.tail)
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
}
