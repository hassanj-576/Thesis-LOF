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


object LOF {
	def main(args: Array[String]) {
		val conf = new SparkConf().setMaster("local").setAppName("My App")
		val sc = new SparkContext(conf)
		val neighbors = getNNeighbors("data2.arff",10,sc)
		val kDistance=getKDistance(neighbors,4)
		val reachDist= getReachDistance(kDistance)
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
	def getKDistance(neighbors:RDD[(Long, Array[(Long, Double)])],k:Integer):RDD[((Long,Double), Array[ Double])]={
		val rejected = neighbors.filter(values=> values._2.size>k)
		println(neighbors.count()-rejected.count())
		println(rejected.count())
		neighbors.first()._2.foreach(println)
		val newNeighbors=rejected.map(values=>(values._1,values._2.map(x=>x._2).zipWithIndex.map(y=>(y._2,y._1))))
		val kDistance = newNeighbors.map(values=> ((values._1,values._2.filter(x=>x._1==k)(0)._2),values._2.map(x=>x._2)))
		kDistance.collect()
		kDistance
	}
	def getReachDistance(kDistance:RDD[((Long,Double), Array[ Double])]):RDD[(Long, Array[ Double])]= {
		// have to calculate this dynamically for each Value based on k
		val reachDist=kDistance.map(values=> (values._1._1,values._2.map(x=>(values._1._2.max(x)))))
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