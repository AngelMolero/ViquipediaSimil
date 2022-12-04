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
    (titulo.substring(2,titulo.length-2), cad.sum)
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

  def mappingEstructurarCosinesim(doc: String, cosinesim: List[(String, Double)]): List[((String, String), Double)] = {
    for (i <- cosinesim) yield ((doc, i._1), i._2)
  }

  def reduccingEstructurarCosinesim(doc: (String, String), cosinesim: List[Double]): ((String,String), Double) = {
    (doc, cosinesim.head)
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
    println("shutdown")
    systema.terminate()
    println("ended shutdown")
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
    InfoFicheros.ficheros = ProcessListStrings.getListOfFiles("viqui_files").map(_.toString)
   // InfoFicheros.numDocumentos = InfoFicheros.ficheros.length
    // Pedir al usuario cantidad de páginas a procesar
    InfoFicheros.numDocumentos = InfoFicheros.ficheros.length

    println("Cantidad de ficheros totales: " + InfoFicheros.numDocumentos)

    // Pedir al usuario cantidad de mappers
    val nmappers = 12
    // Pedir al usuario cantidad de reducers
    val nreducers = 8

    val grupo = for(g <- InfoFicheros.ficheros.zipWithIndex) yield (g._2, List(g._1))

    // MapReduce de ficheros
    println("------------------ MapReduce de ficheros ------------------")
    val ficheros = timeMeasurement(MR("Ficheros", "Ficheros", grupo, mappingLlegir, reduccingLlegir, nmappers, nreducers))

    //Obtener el contenido de los ficheros Map[String, List[String]]
    InfoFicheros.fitxConten = for (f <- ficheros._1) yield (f._1, f._2.head._1)
    InfoFicheros.fitxRefs = for (f <- ficheros._1) yield (f._1, f._2.head._2)
    InfoFicheros.fitxFotos = for (f <- ficheros._1) yield (f._1, f._2.head._3)

    println("------------------ RESULTADO ------------------")
    println("Tiempo de ejecución: " + ficheros._2 / 1000000000.0 + " segundos")

    // MapReduce de Referencia
    println("------------------ MapReduce de Referencias ------------------")
    val reffitxresult = timeMeasurement(MR("MapReduceSystem", "reffitx", InfoFicheros.fitxRefs.toList, mappingRef, reduccingRef, nmappers, nreducers))

    // Ordenar el resultado por número de referencias
    val reffitxresultSorted = reffitxresult._1.toList.sortBy(_._2).reverse
    // Mostrar el resultado
    println("-------------------Resultado-------------------")
    println("Tiempo de ejecución: " + reffitxresult._2 / 1000000000.0 + " segundos")
    println("Cantidad de ficheros devueltos: " + reffitxresultSorted.length)
    println("Cantidad de ficheros que queremos obtener: " + InfoFicheros.numDocumentos)
    //Imprmir titulo y numero de referencias
    reffitxresultSorted.take(10).foreach(x => println("Titulo: "+ x._1 + " -> Referencias:" + x._2))

    if(InfoFicheros.numDocumentos < InfoFicheros.ficheros.length) InfoFicheros.fitxTratar = InfoFicheros.fitxConten.filter(x => reffitxresultSorted.take(InfoFicheros.numDocumentos).map(_._1).contains(x._1)).toList.sortBy(_._1)
    else InfoFicheros.fitxTratar = InfoFicheros.fitxConten.toList.sortBy(_._1)

    println("------------------ MapReduce de Combinaciones sin referencias ------------------")
    val combNoRef = timeMeasurement(MR("MapReduceSystem", "combNoRef", InfoFicheros.fitxTratar, mappingCombNoRef, reduccingCombNoRef, nmappers, nreducers))
    // Ordenar
    InfoFicheros.combinaciones = combNoRef._1.-(" ")

    println("-------------------Resultado-------------------")
    println("Tiempo de ejecución: " + combNoRef._2 / 1000000000.0 + " segundos")
    InfoFicheros.combinaciones.take(10).foreach(x => println("Titulo: "+ x._1 + " -> Titulos:" + x._2.length))

    println("------------------ MapReduce de palabras contadas ------------------")
    val palabrasContadas = timeMeasurement(MR("MapReduceSystem", "palabrasContadas", InfoFicheros.fitxTratar, mappingWC, reduccingWC, nmappers, nreducers))
    //tupla ((titulo, palabra), ocurrencias)
    InfoFicheros.palabrasCont =  palabrasContadas._1.map(x => (x._1, List(x._2))).toList
    println("-------------------Resultado-------------------")
    println("Tiempo de ejecución: " + palabrasContadas._2 / 1000000000.0 + " segundos")
    println("Cantidad de palabras devueltas: " + palabrasContadas._1.size)
    InfoFicheros.palabrasCont.take(10).foreach(x => println("Titulo: "+ x._1._1 + " -> Palabra: " + x._1._2 + " -> Ocurrencias: " + x._2))

    println("------------------ MapReduce de tf ------------------")
    val tf = timeMeasurement(MR("MapReduceSystem", "tf", InfoFicheros.palabrasCont, mappingTF, reduccingTF, nmappers, nreducers))
    //titulo y lista de tuplas (palabra, ocurrencias) (Devuelve)
    println("-------------------Resultado-------------------")
    println("Tiempo de ejecución: " + tf._2 / 1000000000.0 + " segundos")
    println("Cantidad de paginas devueltas: " + tf._1.size)
    tf._1.take(10).foreach(x => println("Titulo: "+ x._1 + " -> Palabra: " + x._2))

    println("------------------ MapReduce de tfidf ------------------")
    val tfidf = timeMeasurement(MR("MapReduceSystem", "tfidf", tf._1.toList, mappingTfIdf, reduccingTfIdf, nmappers, nreducers))
    //palabra y lista de tuplas (titulo, tfidf) (Devuelve)
    println("-------------------Resultado-------------------")
    println("Tiempo de ejecución: " + tfidf._2 / 1000000000.0 + " segundos")
    println("Cantidad de paginas devueltas tfidf: " + tfidf._1.size)
   // tfidf._1.take(10).foreach(x => println("Palabra: "+ x._1 + " -> Titulo: " + x._2.head._1 + " -> tfidf: " + x._2.sortBy(_._2).take(10).reverse))

    println("------------------ MapReduce de girar ------------------")
    val girar = timeMeasurement(MR("MapReduceSystem", "girar", tfidf._1.toList, mappingGirar, reduccingGirar, nmappers, nreducers))
    println("-------------------Resultado-------------------")
    println("Tiempo de ejecución: " + girar._2 / 1000000000.0 + " segundos")
    girar._1.take(10).foreach(x => println("Palabra: "+ x._1 + " -> Titulo: " + x._2.head._1 + " -> tfidf: " + x._2.sortBy(_._2).take(10).reverse))

    InfoFicheros.tituloTFIDF = girar._1

    println("------------------ MapReduce de Denominador ------------------")
    val denominador = timeMeasurement(MR("MapReduceSystem", "denominador", InfoFicheros.tituloTFIDF.toList,  mappigRaizSumatorio, reduccingRaizSumatiorio, nmappers, nreducers))
    println("-------------------Resultado-------------------")
    println("Tiempo de ejecución: " + denominador._2 / 1000000000.0 + " segundos")
    InfoFicheros.raizSumatorio = denominador._1

    println("------------------ MapReduce Similitud documentos ------------------")
    val similitud = timeMeasurement(MR("MapReduceSystem", "similitud", InfoFicheros.combinaciones.toList, mappingCosinoSimil, reduccingCosinoSimil, nmappers, nreducers))
    println("-------------------Resultado-------------------")
    println("Tiempo de ejecución: " + similitud._2 / 1000000000.0 + " segundos")
    //similitud._1.take(10).foreach(x => println("Titulo: "+ x._1 + " -> Titulos:" + x._2.take(10)))

    println("------------------ MapReduce de Similitud Estructura ------------------")
    /*val similitudEstructura = timeMeasurement(MR("MapReduceSystem", "similitudEstructura", similitud._1.toList, mappingEstructurarCosinesim, reduccingEstructurarCosinesim, nmappers, nreducers))
    println("-------------------Resultado-------------------")
    println("Tiempo de ejecución: " + similitudEstructura._2 / 1000000000.0 + " segundos")
    similitudEstructura._1.toList.sortBy(_._2).reverse.take(10).foreach(x => println("Titulo: "+ x._1 + " -> Similitud:" + x._2))*/


    println("------------------ MapReduce de nombre promedio de referencias todas las paginas ------------------")
    val nombrePromRef = timeMeasurement(MR("MapReduceSystem", "nombrePromRef", InfoFicheros.fitxRefs.toList, mappingNombrePromRef, reduccingNombrePromRef, nmappers, nreducers))
    println("-------------------Resultado-------------------")
    println("Tiempo de ejecución: " + nombrePromRef._2 / 1000000000.0 + " segundos")
    println("Numero promedio de referencias: " + nombrePromRef._1.head._2)

    println("------------------ MapReduce de nombre promedio de todas las fotos ------------------")
    val nombrePromedioFotos = timeMeasurement(MR("MapReduceSystem", "nombrePromedioFotos", InfoFicheros.fitxFotos.toList, mappingFotos, reduccingFotos, nmappers, nreducers))
    println("-------------------Resultado-------------------")
    println("Tiempo de ejecución: " + nombrePromedioFotos._2 / 1000000000.0 + " segundos")
    println("Numero promedio de fotos: " + nombrePromedioFotos._1.head._2)
  }
}


