package br.pcontop.assignment

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.jsoup.Jsoup
import java.io._

object Main {

	def main(arg: Array[String]) {

		if (arg.length < 2) {
			System.err.println("Usage: CommonWords <dir book1> <dir book2>")
			System.exit(1)
		}

		val pathToBook1 = arg(0)
		val pathToBook2 = arg(1)
		val jobName = "CommonWords"

		val sc = new SparkContext(new SparkConf().setAppName(jobName))

		def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
			val p = new java.io.PrintWriter(f)
			try { op(p) } finally { p.close() }
		}
		sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive","true")

		//Book a
		var words1 = sc.wholeTextFiles(pathToBook1 + "/**").map{case(_,y) => Jsoup.parse(y).text()}.flatMap{_.split(" ")}

		//words.count()

		var wordCount1 = words1.map{x => (x,1)}.reduceByKey{(x,y) => x + y}

		//Book b
		var words2 = sc.wholeTextFiles(pathToBook2 + "/**").map{case(_,y) => Jsoup.parse(y).text()}.flatMap{_.split(" ")}

		var wordCount2 = words2.map{x => (x,1)}.reduceByKey{(x,y) => x + y}

		var joinBooks = wordCount1.join(wordCount2)

		var joinBooksSum = joinBooks.map{case(word, (one, two)) => (word, one + two)}

		//Final Result
		var result = joinBooksSum.map(a => a.swap).sortByKey(false).map{a => a._2}.take(1500)

		//Printing to file.

		printToFile(new File("wordsInBothBooks.txt")) { p =>  result.foreach(p.println)}

	}
}

