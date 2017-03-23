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
		val fileName=args(0)
		val k = args(1).toInt
		val bucketWidth = args(2).toInt
		val neighbors = getNNeighbors("dataSmall.csv",k,sc,bucketWidth)
		neighbors.filter(values=>values._1==0).first()._2.foreach(println)
		//println(neighbors.count())
		//neighbors.first()._2.foreach(println
	
		// neighbors.collect().foreach(println)
		val kDistance=getKDistance(neighbors,(k-1)))
		//kDistance.collect().foreach(println)
		print(kDistance.lookup(0)(0))
		// val firstRdd= kDistance.filter(values=>values._1._1==0)
		// print(firstRdd.first())
		val localReachDist = getReachDistance(neighbors,kDistance)
		// reachDist.filter(values=>values._1==0).first()._2.foreach(println)
		// val localReachDist=getLocalReachDistance(reachDist)
		// print (localReachDist.filter(values=>values._1==0).first())

		val LOF=getLOF(localReachDist,neighbors)
		val newLOF=LOF.sortBy(_._2)
		newLOF.foreach(println)
		
		//Thread.sleep(500000)
		sc.stop()
		
	}
	def getNNeighbors(fileName:String,minPoints:Int,sc:SparkContext,bucketWidth:Int):RDD[(Long, Array[(Long, Double)])]={
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		val df = sqlContext.read.format("com.databricks.spark.csv").load(fileName)
		val denseVector = df.map(row => {Vectors.dense(row.toSeq.toArray.map({case s: String => s.toDouble case  l: Long => l.toDouble case  _ => 0.0}))})
		val dimension= denseVector.first().size
		val denseRDDZipped = denseVector.map(values=>(values.toSparse)).zipWithIndex()
		val finalVector =denseRDDZipped.map(values=>(values._2,values._1)) 
		val annModel =new ANN(dimensions = dimension, measure = "euclidean").setTables(10).setSignatureLength(64).setBucketWidth(bucketWidth).train(finalVector)
		val neighbors = annModel.neighbors(minPoints)
		neighbors
	
	}
	def getKDistance(neighbors:RDD[(Long, Array[(Long, Double)])],k:Integer):RDD[((Long,Double))]={
		val rejected = neighbors.filter(values=> values._2.size>=k)
		println(rejected.count())
		val newNeighbors=rejected.map(values=>(values._1,values._2.map(x=>x._2).zipWithIndex.map(y=>(y._2,y._1))))
		println(newNeighbors.first())
		val kDistance = newNeighbors.map(values=> ((values._1,values._2.filter(x=>x._1==k)(0)._2)))
		//kDistance.collect()
		kDistance
	}
	//def getReachDistance(neighbors:RDD[(Long, Array[(Long, Double)])],kDistance:RDD[((Long,Double))]):RDD[(Long, Array[ Double])]= {
	def getReachDistance(neighbors:RDD[(Long, Array[(Long, Double)])],kDistance:RDD[((Long,Double))]):RDD[(Long,Double)]= {
		val flatNeighbors = neighbors.flatMapValues(x=>x).map(values=>(values._2._1,(values._1,values._2._2))).join(kDistance).map(y=>(y._2._1._1,y._2._2.max(y._2._1._2)))
		flatNeighbors.filter(values=>values._1==0).foreach(println)
		//flatNeighbors.filter(values=>values._1==4).foreach(println)
		
		val localReachDistance = flatNeighbors.combineByKey((values)=> (values.toDouble, 1),(x:(Double,Int), values)=> (x._1 + values, x._2 + 1), (x:(Double,Int), y:(Double,Int))=>(x._1 + y._1, x._2+ y._2)).map(values=>(values._1,(values._2._2/values._2._1)))
		localReachDistance.filter(values=>values._1==0).foreach(println)
		localReachDistance
		// have to calculate this dynamically for each Value based on k
		//val reachDist=neighbors.map(values=>(values._1,values._2.map(x=>(x._2.max(kDistance.filter(v=>v._1==x._1).first()._2 )))))
		//val reachDist=neighbors.map(values=>(values._1._1,values._2.map(x=>(x._2.max(kDistance.lookup(x._1)(0))))))
		//reachDist 
	} 
	def getLocalReachDistance(reachDist:RDD[(Long, Array[ Double])]):RDD[(Long,Double)]={
		val localReachDist= reachDist.map(values=> (values._1,density(values._2)))
		localReachDist
	}
	def getLOF(reachDist:RDD[(Long, Double)],neighbors:RDD[(Long, Array[(Long, Double)])]):RDD[(Long,Double)]={
		val neighborWithReach=neighbors.join(reachDist).map(values=>((values._1,values._2._2),values._2._1))
		val flat = neighborWithReach.flatMapValues(x=>x).map(values=>(values._2._1,values._1))
		val result=flat.join(reachDist).map(values=>(values._2._1._1,(values._2._1._2,values._2._2)))
		result.filter(values=>values._1==0).foreach(println)
		//val LOF = result.combineByKey((v) => (v._2.toDouble / v._1, 1),(acc: (Double, Int), q:((Double,Double))) => ((q._2/q._1)+acc._1,acc._2+1),(acc1: (Double, Int), acc2: (Double, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)).map(values=>(values._1,(values._2._1/values._2._2)))
		
		val LOF = result.combineByKey((v) => (v._2.toDouble / v._1, 1),(acc: (Double, Int), q:((Double,Double))) => ((q._2/q._1)+acc._1,acc._2+1),(acc1: (Double, Int), acc2: (Double, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
		LOF.filter(values=>values._1==0).foreach(println)
		LOF.map(values=>(values._1,(values._2._1/values._2._2)))
	}
	
	
	def density(xs: Iterable[Double]) = {
		xs.size / xs.sum
	
	}
	
}