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

	testRdd= textFile.map(x=>x+1)
	testRdd.setName("testRdd")
	testRdd.cache()
	println(testRdd.count)

  }

}

//   