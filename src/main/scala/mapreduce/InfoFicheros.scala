package mapreduce

/**
 * Object que creamos como diccionario, para guardar datos que en un futuro del programa nos puede ayudar para operar.
 */
object InfoFicheros {
  var ficheros = List[String]() // Lista de ficheros
  var numDocumentos = 0 // Número de documentos
  var fitxConten =  Map[String, List[String]]() // Mapa de ficheros y sus contenidos
  var fitxRefs =  Map[String, List[String]]() // Mapa de ficheros y sus referencias
  var fitxFotos = Map[String, List[String]]() // Mapa de ficheros y sus fotos
  var fitxTratar = List[(String, List[String])]() // Lista de ficheros y sus contenidos que es con el que tratamos durante el programa
  var palabrasCont = List[((String, String), List[Int])]()  // Lista de ficheros y sus contenidos y las veces que aparece cada palabra. Lo necesitmas a lista la ocurrencia, para luego el paso de parametros en el mapreduce.
  var tituloTFIDF = Map[String, List[(String,Double)]]() // Mapa de titulo y palabra TFIDF
  var combinaciones = Map[String, List[String]]() // Mapa de combinaciones de ficheros
  var raizSumatorio = Map[String, Double]() // Mapa de ficheros y su raiz sumatorio

  /**
   * Función que devuelve un booleano, para saber si existe contiende la referencia.
   * @param titulo1 : título del documento.
   * @param  ref1 : referencia a buscar.
   * @return Boolean : true si existe, false si no existe.
   */
  def existRef(titulo1: String, titulo2: String): Boolean = {
    !fitxRefs(titulo1).contains("[["++titulo2++"]]")
  }

  /**
   * Función que nos alinea los vectores, en el que vamos multiplicando y sumando para obtener el resultado del producto escalar.
   * @param v1 : vector 1 de palabras y tfidf.
   * @param v2 : vector 2 de palabras y tfidf.
   * @param result : variable donde guardaremos el resultado.
   * @return Double : resultado del producto escalar.
   */
  def vectorAlignment(v1: List[(String, Double)], v2: List[(String, Double)], result: Double): Double = {
    if (v1.isEmpty || v2.isEmpty) result
    else {
      val comp = v1.head._1.compareTo(v2.head._1)
      if (comp == 0) vectorAlignment(v1.tail, v2.tail, result + v1.head._2 * v2.head._2)
      else if (comp < 0) vectorAlignment(v1.tail, v2, result)
      else vectorAlignment(v1, v2.tail, result)
    }
  }

  /**
   * Función que nos calcula el cosinesim.
   * @param doc1 : documento 1, que contiene las palabras y el tfidf.
   * @param denomin : denominador del cosinesim.
   * @param i : iterador, el siguiente documento a comparar.
   * @return Double : resultado del cosinesim.
   */
  def cosinesim(doc1: List[(String, Double)], denomin: Double, i: String): Double = {
    val doc2 = InfoFicheros.tituloTFIDF(i)
    val numerador = vectorAlignment(doc1, doc2,0.0)
    val denominador = denomin * InfoFicheros.raizSumatorio(i)
    numerador / denominador
  }
}