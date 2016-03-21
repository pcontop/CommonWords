package br.pcontop.assignment

import java.io._

import org.apache.spark.{SparkConf, SparkContext}
import org.jsoup.Jsoup

object Main {

	def main(arg: Array[String]) {

		if (arg.length < 2) {
			System.err.println("Usage: CommonWords <hdfs dir book1> <hdfs dir book2>")
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
		val words1 = sc.wholeTextFiles(pathToBook1 + "/**").map{case(_,y) => Jsoup.parse(y).text()}.flatMap{_.split(" ")}

		//words.count()

		val wordCount1 = words1.map{x => (x,1)}.reduceByKey{(x,y) => x + y}

		//Book b
		val words2 = sc.wholeTextFiles(pathToBook2 + "/**").map{case(_,y) => Jsoup.parse(y).text()}.flatMap{_.split(" ")}

		val wordCount2 = words2.map{x => (x,1)}.reduceByKey{(x,y) => x + y}

		val joinBooks = wordCount1.join(wordCount2)

		val joinBooksSum = joinBooks.map{case(word, (one, two)) => (word, one + two)}

		//Final Result
		val result = joinBooksSum.map(a => a.swap).sortByKey(ascending = false).map { a => a._2 }.take(1500)

		//Printing to file.

		printToFile(new File("wordsInBothBooks.txt")) { p =>  result.foreach(p.println)}

	}
}

