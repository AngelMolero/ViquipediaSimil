package mapreduce

import scala.util.matching.Regex
import scala.xml.{Elem, XML}

object ViquipediaParse {

  // Fixem el fitxer xml que volem tractar a l'exemple
  val exampleFilename="viqui_files/32509.xml"

  // Definim una case class per a retornar diversos valor, el titol de la pàgina, el contingut i les referències trobades.
  // El contingut, s'ha de polir més? treure refs? stopwords?...
  case class ResultViquipediaParsing(titol: String, contingut: List[String], refs: List[String])

  def testParse= this.parseViquipediaFile(exampleFilename)

  def nonstopfreq(texto: String): List[String] = {
    val stopwords = scala.io.Source.fromFile("stopwordscatalanet.txt").mkString
    val stop = stopwords.split("\n").toList
    val modtexto = texto.toLowerCase().replaceAll("[^a-z]", " ").split(" ").filter(_.nonEmpty)
    val f = modtexto.filter(!stop.contains(_))
    f.toList
  }

  def parseViquipediaFile(filename: String=this.exampleFilename) = {
    val xmlleg = new java.io.InputStreamReader(new java.io.FileInputStream(filename), "UTF-8")

    // Agafo el document XML i ja està internament estructurat per anar accedint als camps que volguem
    val xmllegg: Elem = XML.load(xmlleg)

    // obtinc el titol
    val titol = (xmllegg \\ "title").text

    // obtinc el contingut de la pàgina
    var contingut = (xmllegg \\ "text").text

    // identifico referències
    val ref = new Regex("\\[\\[[^\\]]*\\]\\]")
    //println("La pagina es: " + titol)
    //println("i el contingut: ")
    //println(contingut)
    val refs = (ref findAllIn contingut).toList

    // elimino les que tenen :
    val filteredRefs = refs.filterNot(_.contains(':'))

    // caldrà eliminar-ne més?
    var elim = new Regex("\\[\\[[^\\]]*:[^\\]]*\\]\\]")
    contingut = (elim replaceAllIn (contingut, ""))

    elim = new Regex("\\{\\{[^\\}]*:[^\\}]*\\}\\}")
    contingut = (elim replaceAllIn (contingut, ""))

    elim = new Regex("\\|[^\\]]*\\]\\]")
    contingut = (elim replaceAllIn (contingut, "]"))

    val contingut1 = nonstopfreq(contingut)


    //for (r <- refs) println(r)
    //println(refs.length)
    //println(filteredRefs.length)
    xmlleg.close()
    ResultViquipediaParsing(titol, contingut1, filteredRefs)
  }
}