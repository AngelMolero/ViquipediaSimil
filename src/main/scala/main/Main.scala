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

  def mappingLlegir(id: Int, fitxers: List[String]): List[(String, (List[String], List[String]))] = {
    var llista = List[(String, (List[String], List[String]))]()
    for (f <- fitxers) {
      val parseResult = ViquipediaParse.parseViquipediaFile(f)
      llista = llista :+ (parseResult.titol, (parseResult.contingut, parseResult.refs))
    }
    llista
  }

  def reduccingLlegir(titulo: String, cad: List[(List[String], List[String])]): (String, List[(List[String], List[String])]) = {
    (titulo, List((cad.head._1, cad.head._2)))
  }

  /**
   * Función que mappea un directorio a una lista de ficheros
   * @param fitxer directorio
   * @return lista de ficheros
   */
  def mappingRef(titol: String, info: List[(List[String], List[String])]): List[(String,List[String])] = {
    //for (x <- info.head._2) yield (titol,(x, x))
    List((titol,info.head._2))
  }

  def reduccingRef(titulo: String, cad: List[List[String]]): (String, Int) = {
    (titulo, cad.head.length)
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
    List(((titolPalabra._1,(titolPalabra._2,ocurrencias.head))))
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
  def mappingTfIdf(titol: String, palabrasOcurrencias: List[(String, Int)]): List[(String, (String, Int, Int))] = {
    val n = palabrasOcurrencias.map(_._2).sum
    for (palabraOcurrencia <- palabrasOcurrencias) yield ((palabraOcurrencia._1,(titol,palabraOcurrencia._2,n)))
  }

  /**
   * Función que reduce la lista de tuplas (palabra, (titulo, ocurrencias, total de terminos en el documento))
   * @param palabra palabra.
   * @param cad lista de tuplas (titulo, ocurrencias, total de terminos en el documento).
   * @return tupla (titulo, (palabra, tfidf)).
   */
  def reduccingTfIdf(palabra: String, cad: List[(String, Int, Int)]): (String, List[(String, Double)]) = {
    val tfidf = cad.map(x => (x._1, (x._2.toDouble / x._3.toDouble) * math.log(InfoFicheros.numDocumentos / cad.length)))
    (palabra, tfidf)
  }

  def mappingGirar(palabra: String, tfidf: List[(String, Double)]): List[(String, (String, Double))] = {
    for (x <- tfidf) yield (x._1, (palabra, x._2))
  }

  def reduccingGirar(titulo: String, cad: List[(String, Double)]): (String, List[(String, Double)]) = {
    (titulo, cad.sortBy(_._2))
  }



  //def mappingCombinacionDocs

  /* /**
    * Función que mappea un directorio a una lista de ficheros
    * @param titulo de la pagina
    * @param cont contenido de la pagina que son palabras
    * @return
    */
   def mappingIdf(titulo: String, cont: List[String]): List[(String, Int)]  = {
     for(x <- cont.distinct) yield (x, 1)
   }

   /**
    * Función que reduce el resultado de la función mappingIdf
    * @param palabra palabra
    * @param lista lista de apariciones de la palabra en todos los documentos
    * @return tupla con la palabra y devuelve el calculo idf de la palabra
    */
   def reduccingIdf(palabra: String, lista: List[Int]): (String, Double) = {
     (palabra, math.log(InfoFicheros.numDocumentos /  lista.sum))
   }

   /**
    * Función que mappea la lista de palabras de un fichero
    * @param titulo titulo de la pagina
    * @param cont contenido de la pagina que son palabras
    * @return lista de tuplas con la palabra y el titulo de la pagina y el numero de veces que aparece
    */
   def mappingTfIdf(titulo: String, cont: List[String]): List[((String,String), Int)] = {
     for(x <- cont) yield ((x,titulo), 1)
   }

   /**
    * Función que reduce el resultado de la función mappingTf y calculamos el TfIdf
    * @param titPal palabra y titulo de la pagina
    * @param lista lista de apariciones de la palabra en el documento
    * @return tupla con el titulo y la palabra y devuelve el calculo TfIdf de la palabra
    */
   def reduccingTfIdf(titPal: (String, String), lista: List[Int]): ((String, String), List[Double]) = {
     (titPal, List(lista.sum * InfoFicheros.idf(titPal._1)))
   }*/

  /**
   * Función que mappea la lista de palabras de un fichero
   * @param titPal palabra y titulo de la pagina
   * @param cont TFIDF de la palabra
   * @return
   */
  def mappingPalDoc(titPal: (String, String), cont: List[Double]): List[(String, (String, Double))] = {
    List((titPal._1, (titPal._2, cont.head)))
  }

  def reduccingPalDoc(palabra: String, lista: List[(String, Double)]): (String, List[(String, Double)]) = {
    (palabra, lista)
  }

  // Función que mappea, dado la palabra y la lista de documentos en los que aparece, y su tfidf, queremos obtener ((doci, docj), (tfidfi, tfidfj))
  /*def mappingSimil(palabra: String, cont: List[(String, Double)]): List[((String, String), (Double, Double))] = {
    var llista = List[((String, String), (Double, Double))]()
    for (i <- cont.indices) {
      for (j <- i+1 until cont.length) {
        llista = llista :+ ((cont(i)._1, cont(j)._1), (cont(i)._2, cont(j)._2))
      }
    }
    llista
    //cont.combinations(2).map{case List((a,b), (c,d)) => ((a,c), (b,d))}.toList
    //cont.flatMap(x => cont.map(y => if (x._2 > y._2) ((x._1, y._1), (x._2, y._2)) else ((y._1, x._1), (y._2, x._2))))
  }*/

  /*def mappingCosinoSimil(doc: String, documentos: List[String]): List[(String, (String, Double))] = {
    var llista = List[(String, (String, Double))]()
    val doc1 = InfoFicheros.tituloTFIDF(doc)
    for (i <- documentos) {
      val doc2 = InfoFicheros.tituloTFIDF(i)
      val p = (doc1 ::: doc2).groupBy(_._1).map(a => (a._1, a._2.padTo(2, ("", 0.0))))
      val numerador = p.map(a => (a._1, a._2.foldLeft(1.0)((x, y) => x * y._2))).foldLeft(0.0)((x, y) => x + y._2)
      val denominador = Math.sqrt(doc1.foldLeft(0.0)((x, y) => x + y._2 * y._2).doubleValue) * Math.sqrt(doc2.foldLeft(0.0)((x, y) => x + y._2 * y._2).doubleValue)
      val cosino = numerador / denominador
      llista = llista :+ (doc, (i, cosino))
    }
    llista
  }

  def reduccingCosinoSimil(doc: String, documentos:List[(String, Double)]): (String, List[(String,Double)]) = {
    (doc, documentos)
  }*/
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


  /*def mappingCosinoSimil(doc: String, documentos: List[String]): List[((String, String),(Double, Double))] = {
    val denomin = InfoFicheros.raizSumatorio(doc)
    for (doc2 <- documentos) yield ((doc, doc2), (denomin, InfoFicheros.raizSumatorio (doc2)))
  }

  def reduccingCosinoSimil(doc: (String, String), denominadores:List[(Double, Double)]): ((String, String), Double) = {
    val doc1 = InfoFicheros.tituloTFIDF(doc._1)
    val doc2 = InfoFicheros.tituloTFIDF(doc._2)
    //val numerador = doc1.map(x => doc2.find(y => y._1 == x._1).map(y => x._2 * y._2).getOrElse(0.0)).sum
    val p =  (doc1 ::: doc2).groupBy(_._1).map(a => (a._1, a._2.padTo(2, ("",0.0))))
    val numerador = p.map(a => (a._1, a._2.foldLeft(1.0)((x, y) => x * y._2))).foldLeft(0.0)((x, y) => x + y._2)
    val denominador = denominadores.head._1 * denominadores.head._2
    (doc, numerador / denominador)
  }*/

  //(hola -> ((doc1,5.2)...)   doc1 -> ((hola,5.2))...)              doc1 -> [doc2, doc3, doc4]

  /*def reduccingSimil(doc: (String, String), lista: List[(Double, Double)]): ((String, String), Double) = {
    val v1 = lista.map(_._1)
    val v2 = lista.map(_._2)
    val productoEscalar = v1.zip(v2).map{ case (x1, x2) => x1 * x2 }.sum
    val raizSumatorio = math.sqrt(v1.map(x => x * x).sum) * math.sqrt(v2.map(x => x * x).sum)
    (doc, productoEscalar / raizSumatorio)
  }*/


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
    InfoFicheros.numDocumentos = InfoFicheros.ficheros.length

    println("Cantidad de ficheros totales: " + InfoFicheros.numDocumentos)
    // Pedir al usuario cantidad de páginas a procesar
    val numPag = InfoFicheros.numDocumentos
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

    println("------------------ RESULTADO ------------------")
    println("Tiempo de ejecución: " + ficheros._2 / 1000000000.0 + " segundos")

    // MapReduce de Referencia
    println("------------------ MapReduce de Referencias ------------------")
    // En acabar el MapReduce ens envia un missatge amb el resultat
    //val reffitxresult: Map[String, Int] = MR("MapReduceSystem", "reffitx", ficheros.toList, mappingRef, reduccingRef, nmappers, nreducers)
    // Pasar función de Map Reduce al Time Mesuarament
    val reffitxresult = timeMeasurement(MR("MapReduceSystem", "reffitx", ficheros._1.toList, mappingRef, reduccingRef, nmappers, nreducers))

    // Ordenar el resultado por número de referencias
    val reffitxresultSorted = reffitxresult._1.toList.sortBy(_._2).reverse
    // Mostrar el resultado
    println("-------------------Resultado-------------------")
    println("Tiempo de ejecución: " + reffitxresult._2 / 1000000000.0 + " segundos")
    println("Cantidad de ficheros devueltos: " + reffitxresultSorted.length)
    println("Cantidad de ficheros que queremos obtener: " + numPag)
    //Imprmir titulo y numero de referencias
    val fitxers = reffitxresultSorted.take(10)
    fitxers.foreach(x => println("Titulo: "+ x._1 + " -> Referencias:" + x._2))

    if(numPag < InfoFicheros.numDocumentos) InfoFicheros.fitxTratar = InfoFicheros.fitxConten.filter(x => reffitxresultSorted.take(numPag).map(_._1).contains(x._1)).toList.sortBy(_._1)
    else InfoFicheros.fitxTratar = InfoFicheros.fitxConten.toList.sortBy(_._1)

    println("------------------ MapReduce de Combinaciones sin referencias ------------------")
    val combNoRef = timeMeasurement(MR("MapReduceSystem", "combNoRef", InfoFicheros.fitxTratar, mappingCombNoRef, reduccingCombNoRef, nmappers, nreducers))
    // Ordenar
    InfoFicheros.combinaciones = combNoRef._1.-(" ")

    println("-------------------Resultado-------------------")
    println("Tiempo de ejecución: " + combNoRef._2 / 1000000000.0 + " segundos")
    InfoFicheros.combinaciones.take(10).foreach(x => println("Titulo: "+ x._1 + " -> Titulos:" + x._2.length))
    // imprimir el resultado,  -> Titulos:7373
    /*println("El raro: " + combNoRef._1(Empty))
    println("El raro: " + combNoRef._1.-(null))
    println("El raro: " + combNoRef._1(null).length)
    combNoRef._1.-(null)*/
    // Borrar vacio


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
    tfidf._1.take(10).foreach(x => println("Palabra: "+ x._1 + " -> Titulo: " + x._2.head._1 + " -> tfidf: " + x._2.head._2))

    println("------------------ MapReduce de girar ------------------")
    val girar = timeMeasurement(MR("MapReduceSystem", "girar", tfidf._1.toList, mappingGirar, reduccingGirar, nmappers, nreducers))
    println("-------------------Resultado-------------------")
    println("Tiempo de ejecución: " + girar._2 / 1000000000.0 + " segundos")

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

/*
    println("------------------ MapReduce de similitud de documentos ------------------")
    val similitud = timeMeasurement(MR("MapReduceSystem", "similitud", tfidf._1.toList, mappingSimil, reduccingSimil, nmappers, nreducers))
    // tupla(titulo1,titulo2), double
    println("-------------------Resultado-------------------")
    println("Tiempo de ejecución: " + similitud._2 / 1000000000.0 + " segundos")
    println("Cantidad de paginas devueltas similitud: " + similitud._1.size)
    similitud._1.take(10).foreach(x => println("Titulo1: "+ x._1._1 + " -> Titulo2: " + x._1._2 + " -> Similitud: " + x._2))
*/
   /* println("------------------ MapReduce de idf ------------------")
    // Calcular el idf de cada palabra
  //  val idf = timeMeasurement(MR("MapReduceSystem", "idf", InfoFicheros.fitxConten, mappingIdf, reduccingIdf, nmappers, nreducers))
    InfoFicheros.idf = idf._1
    // Imprimir los 10 primeros idf de cada palabra
    //InfoFicheros.idf.take(10).foreach(println)

    println("------------------ MapReduce de tfidf ------------------")
    // Calcular el tfidf de cada palabra
    val tfidf = timeMeasurement(MR("MapReduceSystem", "tfidf", InfoFicheros.fitxConten, mappingTfIdf, reduccingTfIdf, nmappers, nreducers))*/

    //InfoFicheros.tfidf = tfidf._1

    // Imprimir los 10 primeros tfidf de cada palabra
   // InfoFicheros.tfidf.take(10).foreach(println)

    /*println("------------------ MapReduce de por cada palabra tener una lista de tfidf y documento ------------------")
    // MapReduce para obtener la palabra con la lista de documentos con su tfidf
    val palDocTfidf = timeMeasurement(MR("MapReduceSystem", "palDoc", InfoFicheros.tfidf.toList, mappingPalDoc, reduccingPalDoc, nmappers, nreducers))

    println("------------------ MapReduce de similitud de documentos ------------------")
    // MapReduce para calcular el cosino de similitud entre los documentos
    val similitud = timeMeasurement(MR("MapReduceSystem", "similitud", palDocTfidf._1.toList, mappingSimil, reduccingSimil, nmappers, nreducers))

    // Ordenar el resultado por similitud
    val similitudSorted = similitud._1.toList.sortBy(_._2).reverse
    println("-------------------Resultado-------------------")
    var tiempo = (idf._2 + tfidf._2 + palDocTfidf._2 + similitud._2) / 1000000000.0
    println("Tiempo de ejecución: " + tiempo + " segundos")

    // Imprimir los 10 primeros documentos con su similitud
    similitudSorted.take(10).foreach(println)*/
  }
}


