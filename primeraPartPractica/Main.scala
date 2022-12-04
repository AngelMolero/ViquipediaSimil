import java.lang.Math.round
import scala.::
import scala.collection.immutable.Range
import java.io.File

object Main {
  /**
   * Calcula la frecuencia de las palabras.
   *
   * @param numParaules : número total de palabras.
   * @param i : número de veces que aparece una palabra.
   * @return BigDecimal : frecuencia de la palabra.
   */
  private def calculFreq(numParaules: Int, i: Int): BigDecimal = {
    BigDecimal((i.toFloat / numParaules) * 100)
  }

  /**
   * Calcula la raíz cuadrada de la suma de todos los elementos.
   *
   * @param freq1 : lista con todas las palabras y su frequencia.
   * @return Double : resultado de la raíz cuadrada.
   */
  private def raizSumatorio(freq1: List[(String, Double)]): Double = {
    Math.sqrt(freq1.foldLeft(0.0)((x, y) => x + y._2 * y._2).doubleValue)
  }

  /**
   * Calcula la suma de todos los elementos de una lista.
   *
   * @param l1 : lista de palabras y la cantidad que aparece.
   * @return Int : suma de todos los elementos.
   */
  private def sumatotal(l1: List[(String, Int)]): Int = {
    l1.foldLeft(0)((x, y) => (x + y._2))
  }

  /**
   * Calcula la frecuencia normal.
   *
   * @param l1 : lista de palabras y la cantidad que aparece.
   * @param freqNormMax1 : frecuaencia normal máxima.
   * @param sumaTotal : suma total de apariciones de todas las palabras.
   * @return List[(String, BigDecimal)] : lista de palabras y su frecuencia normal.
   */
  private def frecuenciaNormal(l1: List[(String, Int)], freqNormMax1: Double, sumaTotal: Double): List[(String, Double)] = {
    l1.map(x => (x._1, redondear(BigDecimal((x._2.toFloat / sumaTotal) * 100), 2) / freqNormMax1))
  }

  /**
   * Redondea la frecuencia de las palabras.
   *
   * @param num numero decimal que redondearemos.
   * @param ndigits cantidad de decimales que queremos.
   * @return BigDecimal resultado de redondear
   */
  def redondear(num: BigDecimal, ndigits: Int): Double = {
    num.setScale(ndigits, BigDecimal.RoundingMode.HALF_UP).doubleValue
  }

  /**
   *  Calculamos el producto escalar
   *
   * @param v1 : lista de palabras y su frecuencia.
   * @param v2 : lista de palabras y su frecuencia.
   * @return Double : producto escalar final
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
   *  Calcula la similitud del cosinus entre dos textos.
   * @param t1 : texto1
   * @param t2 : texto2
   * @return BigDecimal : resultado final de la similitud del cosinus.
   */
  def cosinesim(t1: String, t2: String, n: Int = 0): BigDecimal = {
    var l1 = List[(String, Int)]()
    var l2 = List[(String, Int)]()
    if(n == 0){
      l1 = nonstopfreq(t1)
      l2 = nonstopfreq(t2)
    } else {
      l1 = ngrames(n, t1)
      l2 = ngrames(n, t2)
    }
    val totalPal1 = sumatotal(l1)
    val totalPal2 = sumatotal(l2)
    // Calculamos la frecuencia maxima para calcular la frecuencia normalizada de los otros elementos
    val freqNormMax1 = redondear(calculFreq(totalPal1, l1.head._2),2)
    val freqNormMax2 = redondear(calculFreq(totalPal2, l2.head._2),2)
    val freq1 = frecuenciaNormal(l1, freqNormMax1, totalPal1)
    val freq2 = frecuenciaNormal(l2, freqNormMax2, totalPal2)

    // Calculamos el producto escalar
    val numerador = vectorAlignment(freq1.sortBy(_._1), freq2.sortBy(_._1),0.0)//productoEscalar(group)
    // Calculamos el denominador
    val ai = raizSumatorio(freq1)
    val bi = raizSumatorio(freq2)
    redondear(numerador/(ai*bi),3)
  }

  /**
   *  Calcula los n grames de un texto.
   *
   * @param n : tamaño del grama.
   * @param texto : texto del que queremos calcular los n grames.
   * @return List[(String, Int)] : lista de n grames y la cantidad que aparece.
   */
  def ngrames(n: Int, texto: String): List[(String, Int)] = {
    val modtexto = texto.toLowerCase().replaceAll("[^a-z]", " ").split(" ").filter(_.nonEmpty)
    val grames = modtexto.sliding(n).toList
    val nrep = grames.map(x => (x.mkString(" "), 0))
    nrep.groupBy(_._1).map(x => (x._1, x._2.size)).toList.sortWith(_._2 > _._2)
  }

  /**
   * Calcula cuantas palabras aparecen el mismo número de veces.
   *
   * @param texto : texto1
   * @return List[(Int, Int)] : la frecuencia de aparición de los diferentes palabras.
   */
  def paraulafreqfreq(texto: String): List[(Int, Int)] = {
    val f = freq(texto)
    f.groupBy(_._2).map(x => (x._1, x._2.size)).toList.sortWith(_._2 > _._2)
  }

  /**
   *  Calcula la cantidad que aparecen las diferentes palabras del texto sin contar con las stop-words.
   *
   * @param texto : texto a analizar.
   * @return List[(String, Int)] : lista de palabras y la cantidad que aparece.
   */
  def nonstopfreq(texto: String): List[(String, Int)] = {
    val stopwords = scala.io.Source.fromFile("src/main/scala/english-stop.txt").mkString
    val stop = stopwords.split("\n").toList
    val modtexto = texto.toLowerCase().replaceAll("[^a-z]", " ").split(" ").filter(_.nonEmpty)
    val f = modtexto.filter(!stop.contains(_))
    f.distinct.map(p => (p, f.count(x => x == p))).sortWith(_._2 > _._2).toList
  }

  /**
   *  Calcula la cantidad que aparecen las diferentes palabras del texto sin contar con las stop-words.
   *
   * @param texto : texto a analizar.
   * @return List[(String, Int)] : lista de palabras y la cantidad que aparece.
   */
  def freq(texto: String): List[(String, Int)] = {
    val modtexto = texto.toLowerCase().replaceAll("[^a-z]", " ").split(" ").filter(_.nonEmpty).toList
    val palNoRep = modtexto.distinct
    val freq = palNoRep.map(p => (p, modtexto.count(x=>x==p))).sortWith(_._2 > _._2)
    freq
  }

  /**
   * La función printFreq muestra la frecuencia de las palabras leidas.
   * @param frequency una lista de tuplas de tamaño 2 con el elemento de la
   *                  tupla siendo una cadena de caracteres y el segundo un número entero.
   * @return void.
   */
  def printFreq(frequency: List[(String, Int)]): Unit = {
    var numParaules = 0
    frequency.foreach(x => numParaules = numParaules + x._2)
    println("Num de Paraules: " + numParaules + " Diferents: " + frequency.length)
    println("Paraules ocurrencies frequencia")
    println("---------------------------------------")
    for ((str, i) <- frequency.take(10)) {
      val n = calculFreq(numParaules, i)
      println(str + "\t" + i + "\t" + redondear(n,2))
    }
  }

  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def main(args: Array[String]): Unit = {
    val n = 3;
    val files = getListOfFiles("src/main/scala/inputFiles/")

    var resultats: Map[(String, String), BigDecimal] = Map()

    for(i <- files.indices){
      val text1 = scala.io.Source.fromFile(files(i)).mkString

      resultats += ((files(i).getName, files(i).getName) -> 1.000)

      for(j <- i + 1 until files.length){
        val text2 = scala.io.Source.fromFile(files(j)).mkString
        val aux = cosinesim(text1, text2, n)
        resultats += ((files(i).getName, files(j).getName) -> aux)
        resultats += ((files(j).getName, files(i).getName) -> aux)
      }

    }

    print("\t\t\t\t")
    for(i <- 0 until files.length){
      var line = files(i).getName
      while(line.length < 14){
        line += " "
      }
      print(line+"\t")
    }
    println()

    for(i <- files.indices){

      var line = files(i).getName
      while(line.length < 18){
        line += " "
      }
      for(j <- files.indices) {
        line += "%1.3f".format(resultats.find(x => x._1._1 == files(i).getName && x._1._2 == files(j).getName).get._2) + "\t\t\t"
      }
      println(line)
    }



  }



}