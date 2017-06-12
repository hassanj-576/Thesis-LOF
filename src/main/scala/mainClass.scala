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
import scala.collection.mutable.ArrayBuffer


object mainClass {
	def main(args: Array[String]) {
		val fileName=args(0)
		//val k = args(1).toInt
		val bucketWidth = args(1).toInt
		val outputFile = args(2)
		val kList= ArrayBuffer[Int]()
		val k=10;
		for ( a <- 3 to args.length-1){
			kList+=args(a).toInt
		}
		val conf = new SparkConf().setMaster("local").setAppName("My App")
		val sc = new SparkContext(conf)
		val t0 = System.nanoTime()
		val lofWrapper = new LOFWrapper(fileName,kList,sc,bucketWidth)
		val lofVal = lofWrapper.getLOF()
		var z=0
		for (x <- lofVal) {
			x.sortBy(_._2).coalesce(1).saveAsTextFile(outputFile+z)
			z=z+1
		}
		val t1 = System.nanoTime()
		println((t1-t0))
		//Comment from stones
		sc.stop()
		
	}
	
}
