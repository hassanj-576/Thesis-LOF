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


object TestMain2 {
	
	

	def getLOF(faster:Int,fName:String,kPoints:ArrayBuffer[Int],sContext:SparkContext,bWidth:Int):ArrayBuffer[RDD[(Long,Double)]]={
		var fileName = fName
		var kList =kPoints
		var sc=sContext
		var bucketWidth=bWidth
		var fasterCheck=faster
		val sortedList=kList.sortWith(_ > _)
		val LOFList= ArrayBuffer[RDD[(Long,Double)]]()
		val LOFvar = new LOFClass()
		// for( a <- 1 to 2){
		// 	println()
		// 	println()
		// 	println("Iteration: "+a)
		// 	println()
		// 	println()
		// 	// val neighbors = LOFvar.getNNeighbors(fileName,4,sc,bucketWidth)
		// 	// neighbors.setName("superSet")
		// 	// neighbors.first()._2.foreach(println)
		// 	// neighbors.cache
		// 	// val kDistance=LOFvar.getKDistance(neighbors,(3))
		// 	// println(kDistance.count)
		// }
		 for (x <- sortedList) {
		 	println()
			println()
			println("Iteration: "+x)
			println()
			println()
			val nRdd = sc.parallelize(Array(x))
			nRdd.setName("nRdd")
			nRdd.cache
			nRdd.count
		 	val neighbors = LOFvar.getNNeighbors(fileName,x,sc,bucketWidth)
		 	neighbors.setName("superSet")
			//neighbors.first()._2.foreach(println)
			//neighbors.count
			neighbors.first().foreach(println)
			neighbors.cache

			val kDistance=LOFvar.getKDistance(neighbors,x-1)
			val localReachDist = LOFvar.getReachDistance(neighbors,kDistance)
			val LOF=LOFvar.getLOF(localReachDist,neighbors)
			//LOF.take(10).foreach(println)
			println(LOF.first())
			LOFList+=LOF
		} 
		
		LOFList
	}
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
		val lofVal = getLOF(faster,fileName,kList,sc,bucketWidth)
		for (x <- lofVal) {
			x.sortBy(_._2)
		}

		//Comment from stones		
		sc.stop()
		
	}
}
