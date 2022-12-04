package mapreduce

object InfoFicheros {
  var ficheros = List[String]()
  var numDocumentos = 0
  var tfidf = Map[(String, String), List[Double]]()
  var fitxConten =  Map[String, List[String]]()
  var fitxRefs =  Map[String, List[String]]()
  var fitxFotos = Map[String, List[String]]()
  var fitxTratar = List[(String, List[String])]()
  var palabrasCont = List[((String, String), List[Int])]()
  var tituloTFIDF = Map[String, List[(String,Double)]]()
  var combinaciones = Map[String, List[String]]()
  var raizSumatorio = Map[String, Double]()

  def existRef(titulo1: String, titulo2: String): Boolean = {
    !fitxRefs(titulo1).contains("[["++titulo2++"]]")
  }

  def vectorAlignment(v1: List[(String, Double)], v2: List[(String, Double)], result: Double): Double = {
    if (v1.isEmpty || v2.isEmpty) result
    else {
      val comp = v1.head._1.compareTo(v2.head._1)
      if (comp == 0) vectorAlignment(v1.tail, v2.tail, result + v1.head._2 * v2.head._2)
      else if (comp < 0) vectorAlignment(v1.tail, v2, result)
      else vectorAlignment(v1, v2.tail, result)
    }
  }

  def cosinesim(doc1: List[(String, Double)], denomin: Double, i: String): Double = {
    val doc2 = InfoFicheros.tituloTFIDF(i)
    val numerador = vectorAlignment(doc1, doc2,0.0)
    val denominador = denomin * InfoFicheros.raizSumatorio(i)
    numerador / denominador
  }
}
