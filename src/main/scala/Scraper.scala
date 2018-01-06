import java.io.{BufferedWriter, File, FileWriter}

import org.jsoup.Jsoup
import org.rovak.scraper.models.{Href, WebPage}
import org.rovak.scraper.spiders.Spider

import scala.io.Source
object Scraper {

  def parseWebPage(url :String):String={
    val html = Source.fromURL(url)
    Jsoup.parse(html.mkString).text()
  }
  def parseAndSave(startUrl : String,allowedDomain :String, path: String): Unit ={
    new Spider {
      startUrls ::= startUrl
      allowedDomains ::=allowedDomain


      onReceivedPage ::= { page: WebPage =>
      val content = Jsoup.parse(page.doc.outerHtml()).text()
        println("yaaay")

        val file = new File(path +"/"+page.url.toString.replaceAll("[\\./:&?]","_")+".txt")
        println(file.toString)
        file.createNewFile()

        val bw = new BufferedWriter(new FileWriter(file))
        bw.write(content)
        bw.close()
      }

      onLinkFound ::= { link: Href =>
        println(s"Found link ${link.url} with name ${link.name}")
      }

    }.start()

  }



}
