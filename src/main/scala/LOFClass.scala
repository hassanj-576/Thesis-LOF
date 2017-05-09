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
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.DenseVector


class LOFClass () {
	def getNNeighbors(fileName:String,minPoints:Int,sc:SparkContext,bucketWidth:Int):RDD[(Long, Array[(Long, Double)])]={
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		import sqlContext.implicits._
        val df = sqlContext.read.format("com.databricks.spark.csv").load(fileName)
		val doubleDf=df.select((df.columns).map(c => col(c).cast("double")): _*)
		val denseDataFrame = new VectorAssembler().setInputCols(df.columns).setOutputCol("features").transform(doubleDf)
		val denseVector=denseDataFrame.select("features").rdd.map(row=> DenseVector.fromML(row.getAs[org.apache.spark.ml.linalg.DenseVector]("features")))
		val dimension= denseVector.first().size
		val denseRDDZipped = denseVector.map(values=>(values.toSparse)).zipWithIndex()
		val finalVector =denseRDDZipped.map(values=>(values._2,values._1)) 
		val annModel =new ANN(dimensions = dimension, measure = "euclidean").setTables(10).setSignatureLength(64).setBucketWidth(bucketWidth).train(finalVector)
		val neighbors = annModel.neighbors(minPoints)
		neighbors
	
	}
	def getKDistance(neighbors:RDD[(Long, Array[(Long, Double)])],k:Integer):RDD[((Long,Double))]={
		println("k: "+k)
		neighbors.first()._2.foreach(println)
		println(neighbors.first()._2.size)
		val rejected = neighbors.filter(values=> values._2.size>=k)
		println("Rejected Count :"+rejected.count)
		val newNeighbors=rejected.map(values=>(values._1,values._2.map(x=>x._2).zipWithIndex.map(y=>(y._2,y._1))))
		newNeighbors.first()._2.foreach(println)
		newNeighbors.foreach{
			x=>
			if(x._2.size<k){
				println(x._2.size)
			}
		}
		val kDistance = newNeighbors.map(values=> ((values._1,values._2.filter(x=>x._1==k)(0)._2)))
        kDistance
	}

	def getReachDistance(neighbors:RDD[(Long, Array[(Long, Double)])],kDistance:RDD[((Long,Double))]):RDD[(Long,Double)]= {
		val flatNeighbors = neighbors.flatMapValues(x=>x).map(values=>(values._2._1,(values._1,values._2._2))).join(kDistance).map(y=>(y._2._1._1,y._2._2.max(y._2._1._2)))
		val localReachDistance = flatNeighbors.combineByKey((values)=> (values.toDouble, 1),(x:(Double,Int), values)=> (x._1 + values, x._2 + 1), (x:(Double,Int), y:(Double,Int))=>(x._1 + y._1, x._2+ y._2)).map(values=>(values._1,(values._2._2/values._2._1)))
		localReachDistance
		
	} 
	def getLocalReachDistance(reachDist:RDD[(Long, Array[ Double])]):RDD[(Long,Double)]={
		val localReachDist= reachDist.map(values=> (values._1,density(values._2)))
		localReachDist
	}
	def getLOF(reachDist:RDD[(Long, Double)],neighbors:RDD[(Long, Array[(Long, Double)])]):RDD[(Long,Double)]={
		val neighborWithReach=neighbors.join(reachDist).map(values=>((values._1,values._2._2),values._2._1))
		val flat = neighborWithReach.flatMapValues(x=>x).map(values=>(values._2._1,values._1))
		val result=flat.join(reachDist).map(values=>(values._2._1._1,(values._2._1._2,values._2._2)))
		val LOF = result.combineByKey((v) => (v._2.toDouble / v._1, 1),(acc: (Double, Int), q:((Double,Double))) => ((q._2/q._1)+acc._1,acc._2+1),(acc1: (Double, Int), acc2: (Double, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
		LOF.map(values=>(values._1,(values._2._1/values._2._2)))
	}
	
	
	def density(xs: Iterable[Double]) = {
		xs.size / xs.sum
	
	}
	
}
