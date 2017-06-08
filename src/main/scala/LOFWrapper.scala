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
import org.apache.spark.storage.StorageLevel


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
		for (x <- sortedList) {
			println()
			println()
			println("Iteration: "+x)
			println()
			println()
			val nRdd = sc.parallelize(Array(x))
			nRdd.setName("nRdd")
			nRdd.persist(StorageLevel.MEMORY_AND_DISK)
			nRdd.count
		 	val neighbors = LOFvar.getNNeighbors(fileName,x,sc,bucketWidth)
		 	neighbors.setName("superSet")
			neighbors.count
			neighbors.persist(StorageLevel.MEMORY_AND_DISK)
			val kDistance=LOFvar.getKDistance(neighbors,x-1)
			val localReachDist = LOFvar.getReachDistance(neighbors,kDistance)
			val LOF=LOFvar.getLOF(localReachDist,neighbors)
			//LOF.count
			LOFList+=LOF
		} 
		LOFList
	}
}
