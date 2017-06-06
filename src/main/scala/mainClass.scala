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
		val faster=args(0).toInt
		val fileName=args(1)
		//val k = args(1).toInt
		val bucketWidth = args(2).toInt
		val kList= ArrayBuffer[Int]()
		val k=10;
		for ( a <- 3 to args.length-1){
			kList+=args(a).toInt
		}
		println( kList)
		val conf = new SparkConf().setMaster("local").setAppName("My App")
		val sc = new SparkContext(conf)
		val t0 = System.nanoTime()
		val lofWrapper = new LOFWrapper(faster,fileName,kList,sc,bucketWidth)
		val lofVal = lofWrapper.getLOF()
		val t1 = System.nanoTime()
		var z=0
		for (x <- lofVal) {
			x.sortBy(_._2).coalesce(1).saveAsTextFile("newfile"+z+".txt")
			z=z+1
		}

		//Comment from stones	
		println("Over All Total Time : "+(t1-t0))	
		sc.stop()
		
	}
	
}
