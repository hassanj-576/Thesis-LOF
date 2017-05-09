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


class LOFWrapper(faster:Int,fName:String,kPoints:ArrayBuffer[Int],sContext:SparkContext,bWidth:Int) {
	
	private var fileName = fName
	private var kList =kPoints
	private var sc=sContext
	private var bucketWidth=bWidth
	private var fasterCheck=faster

	def getLOF():ArrayBuffer[RDD[(Long,Double)]]={
		val sortedList=kList.sortWith(_ > _)
		val LOFList= ArrayBuffer[RDD[(Long,Double)]]()
		val LOFvar = new LOFClass()
		val neighbors = LOFvar.getNNeighbors(fileName,sortedList(0),sc,bucketWidth)
		val neighborWithzip= neighbors.map(values=>(values._1,values._2.zipWithIndex.map(y=>(y._2,y._1))))
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
		} 
		
		LOFList
	}
}
