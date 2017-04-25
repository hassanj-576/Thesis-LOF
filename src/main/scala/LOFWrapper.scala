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
			val sqlContext = new org.apache.spark.sql.SQLContext(sc)
			val sortedList=kList.sortWith(_ > _)
			val LOFList= ArrayBuffer[RDD[(Long,Double)]]()
			val LOFvar = new LOFClass()
			val neighbors = LOFvar.getNNeighbors(fileName,sortedList(0),sqlContext,bucketWidth)
			val kDistance=LOFvar.getKDistance(neighbors,sortedList(0),sqlContext)
			val localReachDist = LOFvar.getReachDistance(neighbors,kDistance,sqlContext)
			// val LOF=LOFvar.getLOF(localReachDist,filteredNeighbors)
			// LOFList+=LOF
		
		LOFList
	}
}
