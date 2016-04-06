package br.pcontop.assignment

import java.io._

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.jsoup.Jsoup

object Main {

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

	def wordsFromFile(sc: SparkContext, dir: String): RDD[String] = {
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive","true")
    val words1 = sc.wholeTextFiles(dir + "/**").map{case(_,y) => Jsoup.parse(y).text()}.flatMap{_.split(" ")}
    words1
  }

  def wordMap(words: RDD[String]): RDD[(String, Int)] = {
    words.map{x => (x,1)}.reduceByKey{(x,y) => x + y}
  }

  def wordMap(sc: SparkContext, dir: String) : RDD[(String, Int)] = {
    val words = wordsFromFile(sc, dir)
    wordMap(words)
  }

	def main(arg: Array[String]) {

		if (arg.length < 2) {
			System.err.println("Usage: CommonWords <hdfs dir book1> <hdfs dir book2>")
			System.exit(1)
		}

		val pathToBook1 = arg(0)
		val pathToBook2 = arg(1)
		val jobName = "CommonWords"

		val sc = new SparkContext(new SparkConf().setAppName(jobName))

    val wordMap1 = wordMap(sc, pathToBook1)

		val wordMap2 = wordMap(sc, pathToBook2)

		val joinBooks = wordMap1.join(wordMap2)

		val joinBooksSum = joinBooks.map{case(word, (one, two)) => (word, one + two)}

		//Final Result. I have to swap the map to order the RDD by the word occurrence count.
		val result = joinBooksSum.map(a => a.swap).sortByKey(ascending = false).map { a => a._2 }.take(1500)

		//Printing to file.

		printToFile(new File("wordsInBothBooks.txt")) { p =>  result.foreach(p.println)}

	}
}

