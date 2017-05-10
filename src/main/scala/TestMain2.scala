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
		for( a <- 1 to 2){
			val neighbors = LOFvar.getNNeighbors(fileName,4,sc,bucketWidth)
			neighbors.setName("superSet")
			println(neighbors.count())
			neighbors.cache
			val kDistance=LOFvar.getKDistance(neighbors,(4))
			kDistance.foreach(println)
		}

		/*val neighborWithzip= neighbors.map(values=>(values._1,values._2.zipWithIndex.map(y=>(y._2,y._1))))
		var filteredNeighbors=neighbors
		//neighborWithzip.first()._2.foreach(println)
		 for (x <- sortedList) {
			if(x!=sortedList(0)){
				if(fasterCheck==0){
					filteredNeighbors = LOFvar.getNNeighbors(fileName,x,sc,bucketWidth)
				}
				else{
					filteredNeighbors = neighborWithzip.map(values=> (values._1,values._2.filter(z=>z._1<x).map(x=>x._2)))
				}
			}
			val kDistance=LOFvar.getKDistance(filteredNeighbors,(x-1))
			val localReachDist = LOFvar.getReachDistance(filteredNeighbors,kDistance)
			val LOF=LOFvar.getLOF(localReachDist,filteredNeighbors)
			LOFList+=LOF
		} */
		
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
			x.sortBy(_._2).foreach(println)
		}

		//Comment from stones		
		sc.stop()
		
	}
}
