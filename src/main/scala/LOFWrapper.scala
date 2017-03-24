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


class LOFWrapper(fileName:String,k:Int,sc:SparkContext,bucketWidth:Int) {
	
	private var fileName = fileName
	private var k =k
	private var sc=sc
	private var bucketWidth=bucketWidth

	def getLOF():RDD[(Long,Double)]={
		val LOFvar = new LOFClass()
		val neighbors = LOFvar.getNNeighbors(fileName,k,sc,bucketWidth)
		println(neighbors.first())
		val kDistance=LOFvar.getKDistance(neighbors,(k-1))
		val localReachDist = LOFvar.getReachDistance(neighbors,kDistance)
		val LOF=LOFvar.getLOF(localReachDist,neighbors)

	}
}
