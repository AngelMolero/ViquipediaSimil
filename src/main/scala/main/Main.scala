package main

import java.io.File
import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import mapreduce._

import scala.::
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.io.StdIn

object Main {

  def mappingLlegir(id: Int, fitxers: List[String]): List[(String, (List[String], List[String], List[String]))] = {
    var llista = List[(String, (List[String], List[String], List[String]))]()
    for (f <- fitxers) {
      val parseResult = ViquipediaParse.parseViquipediaFile(f)
      llista = llista :+ (parseResult.titol, (parseResult.contingut, parseResult.refs, parseResult.fotos))
    }
    llista
  }

  def reduccingLlegir(titulo: String, cad: List[(List[String], List[String], List[String])]): (String, List[(List[String], List[String], List[String])]) = {
    (titulo, List((cad.head._1, cad.head._2, cad.head._3)))
  }

  /**
   * Función que mappea un directorio a una lista de ficheros
   * @param fitxer directorio
   * @return lista de ficheros
   */
  def mappingRef(titol: String, info: List[String]): List[(String,Int)] = {
   for (i <- info) yield (i,1)
  }

  def reduccingRef(titulo: String, cad: List[Int]): (String, Int) = {
    val t = titulo.substring(2,titulo.length-2)
    if (InfoFicheros.fitxConten.contains(t)) (t, cad.sum)
    else (titulo, 0)
  }


  def mappingCombNoRef(titol: String, palabras: List[String]): List[(String, String)] = {
    val l = InfoFicheros.fitxTratar.drop(InfoFicheros.fitxTratar.indexWhere(_._1 == titol))
    for (x <- l) yield if(!titol.equals(x._1) && InfoFicheros.existRef(titol, x._1)) (titol, x._1) else (" ","")
  }

  def reduccingCombNoRef(titulo: String, cad: List[String]): (String, List[String]) = {
    (titulo, cad)
  }


  /**
   * Función que mappea la ocurrencias de una palabra en un fichero
   * @param titol titulo del fichero
   * @param contingut contenido del fichero
   * @return lista de tuplas ((titulo, palabra), 1)
   */
  def mappingWC(titol: String, contingut: List[String]): List[((String, String), Int)] = {
    for (paraula <- contingut) yield ((titol,paraula),1)
  }

  /**
   * Función que reduce la ocurrencias de una palabra en un fichero
   * @param titol titulo del fichero
   * @param cad lista de tuplas ((titulo, palabra), 1)
   * @return tupla ((titulo, palabra), ocurrencias)
   */
  def reduccingWC(titol: (String, String), cad: List[Int]): ((String, String), Int) = {
    (titol, cad.sum)
  }

  /**
   * Función que mappea cada documento y su frecuencia de una palabra
   * @param titolPalabra titulo y palabra
   * @param ocurrencias ocurrencias de la palabra en el documento
   * @return lista de tuplas (titulo, (palabra, ocurrencias))
   */
  def mappingTF(titolPalabra: (String,String), ocurrencias: List[Int]): List[(String, (String, Int))] = {
    List((titolPalabra._1,(titolPalabra._2,ocurrencias.head)))
  }

  /**
   * Función que devuelve tuplas clave-valor como la clave (titulo, total de terminos del documento)
   * y las tuplas (palabra, ocurrencias) como valor.
   * @param titol titulo del documento.
   * @param cad lista de tuplas (palabra, ocurrencias).
   * @return tupla (titulo, total de terminos del documento) y lista de tuplas (palabra, ocurrencias).
   */
  def reduccingTF(titol: String, cad: List[(String, Int)]): (String,List[(String, Int)]) = {
    (titol,cad)
  }

  /**
   * @param titolNumero tupla (titulo, total de terminos del documento).
   * @param palabrasOcurrencias lista de tuplas (palabra, ocurrencias).
   * @return lista de tuplas (palabra, (titulo, ocurrencias, total de terminos en el documento)).
   */
  def mappingTfIdf(titol: String, palabrasOcurrencias: List[(String, Int)]): List[(String, (String, Int))] = {
    for (palOcur <- palabrasOcurrencias) yield ((palOcur._1,(titol,palOcur._2)))
  }

  /**
   * Función que reduce la lista de tuplas (palabra, (titulo, ocurrencias, total de terminos en el documento))
   * @param palabra palabra.
   * @param cad lista de tuplas (titulo, ocurrencias, total de terminos en el documento).
   * @return tupla (titulo, (palabra, tfidf)).
   */
  def reduccingTfIdf(palabra: String, cad: List[(String, Int)]): (String, List[(String, Double)]) = {
    val tfidf = cad.map(x => (x._1, x._2.toDouble * math.log(InfoFicheros.numDocumentos / cad.length)))
    (palabra, tfidf)
  }

  def mappingGirar(palabra: String, tfidf: List[(String, Double)]): List[(String, (String, Double))] = {
    for (x <- tfidf) yield (x._1, (palabra, x._2))
  }

  def reduccingGirar(titulo: String, cad: List[(String, Double)]): (String, List[(String, Double)]) = {
    (titulo, cad.sortBy(_._2))
  }

  def mappigRaizSumatorio(titulo: String, cont: List[(String, Double)]): List[(String, Double)] = {
    for (i <- cont) yield (titulo, i._2)
  }

  def reduccingRaizSumatiorio(titulo: String, cont: List[Double]): (String, Double) = {
    (titulo, Math.sqrt(cont.foldLeft(0.0)((x, y) => x + y * y).doubleValue))
  }

  def mappingCosinoSimil(doc: String, documentos: List[String]): List[(String, (String, Double))] = {
    val doc1 = InfoFicheros.tituloTFIDF(doc)
    val denomin = InfoFicheros.raizSumatorio(doc)
    for (i <- documentos) yield (doc, (i, InfoFicheros.cosinesim(doc1, denomin, i)))
  }

  def reduccingCosinoSimil(doc:  String, cosinesim: List[(String, Double)]): (String, List[(String,Double)]) = {
    (doc, cosinesim)
  }

  def mappingFotos(titol: String, fotos: List[String]): List[(Int, Int)] = {
    List((1, fotos.length))
  }

  def reduccingFotos(titol: Int, fotos: List[Int]): (Int, Double) = {
    (titol, fotos.sum/InfoFicheros.numDocumentos)
  }

  def mappingNombrePromRef(titol: String, ref: List[String]): List[(Int, Int)] = {
    List((1, ref.length))
  }

  def reduccingNombrePromRef(titol: Int, ref: List[Int]): (Int, Double) = {
    (titol, ref.sum/InfoFicheros.numDocumentos)
  }

  private def calculoTfIdf(nmappers: Int, nreducers: Int): (Map[String, List[(String, Double)]], Double) = {

    // Calculo de ocurrencias de la palabra en cada documento
    var seg = ocurrenciasPalabras(nmappers, nreducers)

    // Calculo de TF
    val tf = calculoTf(nmappers, nreducers)

    seg += tf._2 / 1000000000.0

    // Calculo TFIDF
    val tfidfs = tfidf(nmappers, nreducers, tf)

    (tfidfs._1, seg + tfidfs._2 / 1000000000.0)
  }

  private def tfidf(nmappers: Int, nreducers: Int, tf: (Map[String, List[(String, Int)]], Long)): (Map[String, List[(String, Double)]], Long) = {
    println("------------------ MapReduce de tfidf ------------------")
    val tfidf = timeMeasurement(MR("MapReduceSystem", "tfidf", tf._1.toList, mappingTfIdf, reduccingTfIdf, nmappers, nreducers))
    //palabra y lista de tuplas (titulo, tfidf) (Devuelve)
    println("-------------------Resultado-------------------")
    println("Tiempo de ejecución: " + tfidf._2 / 1000000000.0 + " segundos")
    tfidf
  }

  private def calculoTf(nmappers: Int, nreducers: Int): (Map[String, List[(String, Int)]], Long) = {
    println("------------------ MapReduce de tf ------------------")
    val tf = timeMeasurement(MR("MapReduceSystem", "tf", InfoFicheros.palabrasCont, mappingTF, reduccingTF, nmappers, nreducers))
    //titulo y lista de tuplas (palabra, ocurrencias) (Devuelve)
    println("-------------------Resultado-------------------")
    println("Tiempo de ejecución: " + tf._2 / 1000000000.0 + " segundos")
    tf
  }

  private def ocurrenciasPalabras(nmappers: Int, nreducers: Int): Double = {
    println("------------------ MapReduce de palabras contadas ------------------")
    val palabrasContadas = timeMeasurement(MR("MapReduceSystem", "palabrasContadas", InfoFicheros.fitxTratar, mappingWC, reduccingWC, nmappers, nreducers))
    //tupla ((titulo, palabra), ocurrencias)
    InfoFicheros.palabrasCont = palabrasContadas._1.map(x => (x._1, List(x._2))).toList
    println("-------------------Resultado-------------------")
    val tiempo = palabrasContadas._2 / 1000000000.0
    println("Tiempo de ejecución: " + tiempo + " segundos")
    tiempo
  }

  private def combSinRep(nmappers: Int, nreducers: Int): Double = {
    println("------------------ MapReduce de Combinaciones sin referencias ------------------")
    val combNoRef = timeMeasurement(MR("MapReduceSystem", "combNoRef", InfoFicheros.fitxTratar, mappingCombNoRef, reduccingCombNoRef, nmappers, nreducers))
    // Ordenar
    InfoFicheros.combinaciones = combNoRef._1.-(" ")

    println("-------------------Resultado-------------------")
    val tiempo = combNoRef._2 / 1000000000.0
    println("Tiempo de ejecución: " + tiempo + " segundos")
    tiempo
  }

  /**
   * Función que llama al MapReduce para calcular los documentos que esten mas referenciados.
   * @param nmappers Número de mappers que se van a utilizar.
   * @param nreducers Número de reducers que se van a utilizar.
   * @return Devuelve una lista de tuplas (titulo, numero de referencias).
   */
  private def lasMasReferenciadas(nmappers: Int, nreducers: Int): (List[(String, Int)], Double) = {
    println("------------------ MapReduce de Referencias ------------------")
    val reffitxresult = timeMeasurement(MR("MapReduceSystem", "reffitx", InfoFicheros.fitxRefs.toList, mappingRef, reduccingRef, nmappers, nreducers))

    // Ordenar el resultado por número de referencias
    val reffitxresultSorted = reffitxresult._1.-(" ").toList.sortBy(_._2).reverse
    // Mostrar el resultado
    println("-------------------Resultado-------------------")
    val tiempo = reffitxresult._2 / 1000000000.0
    println("Tiempo de ejecución: " + tiempo + " segundos")
    println("Cantidad de ficheros devueltos: " + reffitxresultSorted.length)
    //Imprmir titulo y numero de referencias
    reffitxresultSorted.take(10).foreach(x => println("Titulo: " + x._1 + " -> " + x._2 + " referencias"))
    (reffitxresultSorted, tiempo)
  }

  /**
   * Función que realiza una lectura de ficheros para obtener los datos de los ficheros
   * @param nmappers Número de mappers
   * @param nreducers Número de reducers
   * @param grupo Grupo de ficheros a leer
   * @return
   */
  private def lecturaFicheros(nmappers: Int, nreducers: Int, grupo: List[(Int, List[String])]): Double = {
    // MapReduce de ficheros
    println("------------------ MapReduce de ficheros ------------------")
    val ficheros = timeMeasurement(MR("Ficheros", "Ficheros", grupo, mappingLlegir, reduccingLlegir, nmappers, nreducers))

    //Obtener el contenido de los ficheros Map[String, List[String]]
    InfoFicheros.fitxConten = for (f <- ficheros._1) yield (f._1, f._2.head._1)
    InfoFicheros.fitxRefs = for (f <- ficheros._1) yield (f._1, f._2.head._2)
    InfoFicheros.fitxFotos = for (f <- ficheros._1) yield (f._1, f._2.head._3)

    println("------------------ RESULTADO ------------------")
    val tiempo = ficheros._2 / 1000000000.0
    println("Tiempo de ejecución: " + tiempo + " segundos")
    tiempo
  }

  /**
   * Función que calcula llama a un mapreduce para calcular el promedio de fotos.
   * @param nmappers Número de mappers
   * @param nreducers Número de reducers
   * @return Lista de tuplas (nº grupo, lista de ficheros)
   */
  private def promedioDeFotos(nmappers: Int, nreducers: Int): Double = {
    println("------------------ MapReduce de nombre promedio de todas las fotos ------------------")
    val nombrePromedioFotos = timeMeasurement(MR("MapReduceSystem", "nombrePromedioFotos", InfoFicheros.fitxFotos.toList, mappingFotos, reduccingFotos, nmappers, nreducers))
    println("-------------------Resultado-------------------")
    val tiempo = nombrePromedioFotos._2 / 1000000000.0
    println("Tiempo de ejecución: " + tiempo + " segundos")
    println("Numero promedio de fotos: " + nombrePromedioFotos._1.head._2)
    tiempo
  }

  /**
   * Función para calcular el promedio de referencias
   * @param nmappers Número de mappers
   * @param nreducers Número de reducers
   * @return Tiempo de ejecución
   */
  private def promedioDeReferencias(nmappers: Int, nreducers: Int): Double = {
    println("------------------ MapReduce de nombre promedio de referencias todas las paginas ------------------")
    val nombrePromRef = timeMeasurement(MR("MapReduceSystem", "nombrePromRef", InfoFicheros.fitxRefs.toList, mappingNombrePromRef, reduccingNombrePromRef, nmappers, nreducers))
    println("-------------------Resultado-------------------")
    val tiempo = nombrePromRef._2 / 1000000000.0
    println("Tiempo de ejecución: " + tiempo + " segundos")
    println("Numero promedio de referencias: " + nombrePromRef._1.head._2)
    tiempo
  }

  /**
   * Función para calcular la similitud entre documentos, primero se calcula el denominador de la fórmula
   * y luego se calcula el numerador, para finalmente calcular la similitud
   * @param nmappers numero de mappers
   * @param nreducers numero de reducers
   * @return
   */
  private def calculoSimilitud(nmappers: Int, nreducers: Int): Double = {
    println("------------------ MapReduce de Denominador ------------------")
    val denominador = timeMeasurement(MR("MapReduceSystem", "denominador", InfoFicheros.tituloTFIDF.toList, mappigRaizSumatorio, reduccingRaizSumatiorio, nmappers, nreducers))
    println("-------------------Resultado-------------------")
    println("Tiempo de ejecución: " + denominador._2 / 1000000000.0 + " segundos")
    InfoFicheros.raizSumatorio = denominador._1

    println("------------------ MapReduce Similitud documentos ------------------")
    val similitud = timeMeasurement(MR("MapReduceSystem", "similitud", InfoFicheros.combinaciones.toList, mappingCosinoSimil, reduccingCosinoSimil, nmappers, nreducers))
    println("-------------------Resultado-------------------")
    val tiempo = similitud._2 / 1000000000.0
    println("Numero de documentos: " + InfoFicheros.numDocumentos)
    println("Tiempo de ejecución: " + tiempo + " segundos")
    val similMax = similitud._1.view.mapValues(_.maxBy(_._2)).toList.sortBy(_._2._2).reverse
    similMax.take(5).foreach(x => println(x._1 + " - " + x._2._1 + " -> " + x._2._2))
    tiempo
  }

  /**
   * Función que usamos para interambiar la key por el value
   * @param nmappers Número de mappers
   * @param nreducers Número de reducers
   * @param tfidfs Mapa con el titulo y el tfidf
   * @return los segundos que ha tardado en ejecutarse
   */
  private def girarLaKeyConValue(nmappers: Int, nreducers: Int, tfidfs: (Map[String, List[(String, Double)]], Double)): Double = {
    println("------------------ MapReduce de girar ------------------")
    val girar = timeMeasurement(MR("MapReduceSystem", "girar", tfidfs._1.toList, mappingGirar, reduccingGirar, nmappers, nreducers))
    println("-------------------Resultado-------------------")
    val tempo = girar._2 / 1000000000.0
    println("Tiempo de ejecución: " + tempo + " segundos")
    InfoFicheros.tituloTFIDF = girar._1
    tempo
  }

  /**
   * Función generica para realizar un MapReduce
   * @param actorSystem Nombre del actorSystem
   * @param actorname Nombre del actor
   * @param input Datos de entrada
   * @param mapping Función de mapeo
   * @param reduccing Función de reducción
   * @param nmappers Número de mappers
   * @param nreducers Número de reducers
   * @return Resultado del MapReduce
   */
  def MR[K1, V1, K2, V2, V3](actorSystem: String, actorname: String, input:  List[(K1, List[V1])], mapping: (K1, List[V1]) => List[(K2, V2)], reduccing: (K2, List[V2]) => (K2, V3), nmappers: Int, nreducers: Int): Map[K2, V3] = {
    // Crear el actor system
    val systema: ActorSystem = ActorSystem(actorSystem)

    // Crear el actor MapReduce
    val actor = systema.actorOf(Props(new MapReduce[K1,V1,K2,V2,V3](input, mapping, reduccing, nmappers, nreducers)), actorname)
    implicit val timeout = Timeout(10000 seconds) // L'implicit permet fixar el timeout per a la pregunta que enviem al actor. És obligagori.
    val future = actor ? MapReduceCompute() // Enviem el missatge MapReduceCompute al actor

    println("Awaiting result...")
    // En acabar el MapReduce ens envia un missatge amb el resultat
    val result: Map[K2, V3] = Await.result(future, timeout.duration).asInstanceOf[Map[K2, V3]]

    // Fem el shutdown del actor system
    systema.terminate()
    // com tancar el sistema d'actors.
    result
  }

  /**
   * Función que calcula el tiempo que ha tardado una función en ejecutarse
   * @param expr función que queremos calcular el tiempo
   * @tparam A tipo de la función
   * @return tupla con el tiempo que ha tardado la función y el resultado de la función
   */
  def timeMeasurement[A](expr: => A): (A, Long) = {
    val t0 = System.nanoTime()
    val result = expr
    val t1 = System.nanoTime()
    (result, t1 - t0)
  }

  def main(args: Array[String]): Unit = {
    // Obtenemos los nombres de los ficheros
    InfoFicheros.ficheros = ProcessListStrings.getListOfFiles("viqui_files").map(_.toString)

    // Numero de documentos que queremos procesar
    val numPag = InfoFicheros.ficheros.length
    // Pedir al usuario cantidad de mappers
    val nmappers = 12
    // Pedir al usuario cantidad de reducers
    val nreducers = 8

    println("Cantidad de ficheros totales: " + InfoFicheros.ficheros.length)

    val grupo = for(g <- InfoFicheros.ficheros.zipWithIndex) yield (g._2, List(g._1))

    //Map Reduce para obtener ficheros el fichero
    var segons = lecturaFicheros(nmappers, nreducers, grupo)

    // MapReduce de Referencia
    val (reffixResult, s) = lasMasReferenciadas(nmappers, nreducers)

    // Sumamos segundos;
    segons += s

    //Obtenemos la cantidad de ficheros que pida el usuario. Si no se pide ninguno, se obtienen todos
    if (numPag < InfoFicheros.ficheros.length) InfoFicheros.fitxTratar = InfoFicheros.fitxConten.filter(x => reffixResult.take(numPag).map(_._1).contains(x._1)).toList.sortBy(_._1)
    else InfoFicheros.fitxTratar =  InfoFicheros.fitxConten.toList.sortBy(_._1)

    // Obtenemos el numero de documentos que hay
    InfoFicheros.numDocumentos = InfoFicheros.fitxTratar.length

    // Map Reduce para combinar los ficheros sin repeticiones
    var seg = combSinRep(nmappers, nreducers)
    //Sumamos segundos;
    segons += seg

    // Map Reduces Calculamos TDIFIF con Map Reduce
    val tfidfs = calculoTfIdf(nmappers, nreducers)

    segons += tfidfs._2

    // Map Reduce: Ponemos el titulo como key y la palabra junto el tfidf como value
    segons += girarLaKeyConValue(nmappers, nreducers, tfidfs)

    // Map Reduces: calculo de Similitud
    segons += calculoSimilitud(nmappers, nreducers)

    // Map Reduces: calculo de promedio de referencias
    segons += promedioDeReferencias(nmappers, nreducers)

    // Map Reduces: calculo de promedio de fotos
    segons += promedioDeFotos(nmappers, nreducers)

    println("Tiempo total: " + segons + " segundos")
  }
}