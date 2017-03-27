package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import collection.JavaConversions._
import org.apache.spark.SparkContext._
import java.io.StringReader
import com.opencsv.CSVReader;
import com.github.karlhigley.spark.neighbors.ANN
import org.apache.spark.rdd.RDD


object mainClass {
	def main(args: Array[String]) {
		val fileName=args(0)
		val k = args(1).toInt
		val bucketWidth = args(2).toInt
		val conf = new SparkConf().setMaster("local").setAppName("My App")
		val sc = new SparkContext(conf)
		val lofWrapper = new LOFWrapper(fileName,k,sc,bucketWidth)
		val lofVal = lofWrapper.getLOF()
		lofVal.foreach(println)

		//Comment from stones		
		sc.stop()
		
	}
	
}