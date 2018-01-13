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
        val fullPath: String = "/"+page.url.toString.replaceAll("[\\./:&?]","_")+".txt"
        val file = new File(path +fullPath)
        println(file.toString)
        file.createNewFile()

        val file1 = new File("urls"+fullPath)
        file1.createNewFile()

        val bw = new BufferedWriter(new FileWriter(file))
        bw.write(content)
        bw.close()


        val bw1 = new BufferedWriter(new FileWriter(file1))
        bw1.write("file:"+new File("").getAbsolutePath+"/"+path+fullPath+";"+page.url.toString)
        bw1.close()
      }

      onLinkFound ::= { link: Href =>
        println(s"Found link ${link.url} with name ${link.name}")
      }

    }.start()

  }



}
