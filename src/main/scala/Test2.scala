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


object Test2 {
	def main(args: Array[String]) {
		val conf = new SparkConf().setMaster("local").setAppName("My App")
		val sc = new SparkContext(conf)
		val neighbors = getNNeighbors("data2.arff",10,sc)
		val reachDist= getReachDistance(neighbors,5)
		val localReachDist=getLocalReachDistance(reachDist)
		val LOF=getLOF(localReachDist,neighbors)
		LOF.collect().foreach(println)
		sc.stop()
		
	}
	def getNNeighbors(fileName:String,minPoints:Int,sc:SparkContext):RDD[(Long, Array[(Long, Double)])]={
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		val df = sqlContext.read.format("com.databricks.spark.csv").load(fileName)
		val denseVector = df.map(row => {Vectors.dense(row.toSeq.toArray.map({case s: String => s.toDouble case  l: Long => l.toDouble case  _ => 0.0}))})
		val dimension= denseVector.first().size
		val denseRDDZipped = denseVector.map(values=>(values.toSparse)).zipWithIndex()
		val FinalVector =denseRDDZipped.map(values=>(values._2,values._1)) 
		val annModel =new ANN(dimensions = dimension, measure = "euclidean").setTables(4).setSignatureLength(64).setBucketWidth(5).train(FinalVector)
		val neighbors = annModel.neighbors(minPoints)
		neighbors
	
	}
	def getReachDistance(neighbors:RDD[(Long, Array[(Long, Double)])],k:Integer):RDD[(Long, Array[ Double])]= {
		// have to calculate this dynamically for each Value based on k
		val kDistance =0.02
		val reachDist=neighbors.map(values=> (values._1,values._2.map(x=>(kDistance.max(x._2)))))
		reachDist 
	} 
	def getLocalReachDistance(reachDist:RDD[(Long, Array[ Double])]):RDD[(Long,Double)]={
		val localReachDist= reachDist.map(values=> (values._1,density(values._2)))
		localReachDist
	}
	def getLOF(reachDist:RDD[(Long, Double)],neighbors:RDD[(Long, Array[(Long, Double)])]):RDD[(Long,Double)]={
		val neighborWithReach=neighbors.join(reachDist).map(values=>((values._1,values._2._2),values._2._1))
		val flat = neighborWithReach.flatMapValues(x=>x).map(values=>(values._2._1,values._1))
		val result=flat.join(reachDist).map(values=>(values._2._1._1,(values._2._1._2,values._2._2)))
		val LOF = result.combineByKey((v) => (v._2.toDouble / v._1, 1),(acc: (Double, Int), q:((Double,Double))) => ((q._2/q._1)+acc._1,acc._2+1),(acc1: (Double, Int), acc2: (Double, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)).map(values=>(values._1,(values._2._1/values._2._2)))
		LOF
	}
	
	
	def density(xs: Iterable[Double]) = {
		xs.size / xs.sum
	
	}
	
}