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
import org.apache.spark.sql.functions.udf


class LOFClass () {
	def getNNeighbors(fileName:String,minPoints:Int,sqlContext:SQLContext,bucketWidth:Int):DataFrame={
		//Change
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
		val neighborsDF = neighbors.toDF()
		neighborsDF
	
	}
	def getKDistance(neighborsDF:DataFrame,k:Integer,sqlContext:SQLContext):DataFrame={
		import sqlContext.implicits._
		val rejected=neighborsDF.where("size(_2)=="+k)
		rejected.registerTempTable("df")
		val kDistance= sqlContext.sql("SELECT _1,_2["+(k-1)+"]['_2'] FROM df")
		kDistance
	}

	def getReachDistance(neighbors:DataFrame,kDistance:DataFrame,sqlContext:SQLContext):DataFrame= {
		import sqlContext.implicits._
		neighbors.registerTempTable("nTemp")
		sqlContext.udf.register("maxUDF", maxFunction , Array[(Long,Double)])
		sqlContext.sql("SELECT _1, maxUDF(_2) FROM nTemp").show()
		neighbors
		// neighbors.withColumn("upper", maxUDF(_2)).show
		// neighbors
		// val flatNeighbors = neighbors.flatMapValues(x=>x).map(values=>(values._2._1,(values._1,values._2._2))).join(kDistance).map(y=>(y._2._1._1,y._2._2.max(y._2._1._2)))
		// val localReachDistance = flatNeighbors.combineByKey((values)=> (values.toDouble, 1),(x:(Double,Int), values)=> (x._1 + values, x._2 + 1), (x:(Double,Int), y:(Double,Int))=>(x._1 + y._1, x._2+ y._2)).map(values=>(values._1,(values._2._2/values._2._1)))
		// localReachDistance
		
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
	
	def maxFunction(neighborVal:Array[(Long,Double)]): Array[(Long,Double)] = { 
		var max=0;
		neighborVal.foreach(
			(id: Long, distance:Double) => 
			println(distance)
			neighborVal(id,10)
			)
		neighborVal
  		
	} 
	def density(xs: Iterable[Double]) = {
		xs.size / xs.sum
	
	}
	
}
