package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import collection.JavaConversions._
import org.apache.spark.rdd.RDD


object TestMain {
  
  def main(args: Array[String]) {
  	val conf = new SparkConf().setMaster("local").setAppName("My App")
	val sc = new SparkContext(conf)
  	println("Hello World")
  	val textFile = sc.textFile("rddInput.txt")
  	println(textFile.count())
  	type rddType = (Long)
	var testRdd = sc.emptyRDD[rddType]
	var fileName="rddInput.txt"
	for( a <- 1 to 2){
		if(a==2){
			fileName="rddInput2.txt"
		}
		val textFile = sc.textFile(fileName)
		testRdd= textFile.map(x=>x.toInt+1)
		testRdd.cache()
		println(testRdd.count)
		val newTestRdd = testRdd.map(x=>x.toInt+1)
		testRdd.setName("testRdd")
		//println(testRdd.count)
		println("Iteration: "+a)
		//testRdd.foreach(x=> println("Value: "+x))
		//println("Test RDD Size: "+testRdd.count)
		// println("Printing the entire thing")
		// testRdd.foreach(println)
		// println("Printing the entire thing Second Time")
		// testRdd.foreach(println)
		println("Printing new test rdd")
		newTestRdd.foreach(println)
	}

  }

}

//   