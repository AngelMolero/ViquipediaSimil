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

  def cosinesim(doc1: List[(String, Double)], denomin: Double, i: String): Double ={
    val doc2 = InfoFicheros.tituloTFIDF(i)
    val p = (doc1 ::: doc2).groupBy(_._1).map(a => (a._1, a._2.padTo(2, ("", 0.0))))
    val numerador = p.map(a => (a._1, a._2.foldLeft(1.0)((x, y) => x * y._2))).foldLeft(0.0)((x, y) => x + y._2)
    val denominador = denomin * InfoFicheros.raizSumatorio(i)
    numerador / denominador
  }
}
