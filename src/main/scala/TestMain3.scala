// package main.scala

// import org.apache.spark.SparkConf
// import org.apache.spark.SparkContext
// import org.apache.spark.SparkContext._
// import collection.JavaConversions._
// import org.apache.spark.rdd.RDD


// object TestMain3 {
  
//   def main(args: Array[String]) {
//   	val conf = new SparkConf().setMaster("local").setAppName("My App")
// 	val sc = new SparkContext(conf)
//   	println("Hello World")
//   	val textFile = sc.textFile("rddInput.txt")
//   	println(textFile.count())
//   	type rddType = (Long)
// 	var testRdd = sc.emptyRDD[rddType]
// 	var fileName="rddInput.txt"
// 	for( a <- 1 to 5){
// 		val nRdd = sc.parallelize(Array(a))
// 		nRdd.setName("nRdd")
// 		nRdd.cache
// 		nRdd.count

// 		var nInt=0
// 		for ((id: Int,rdd: org.apache.spark.rdd.RDD[_])<- sc.getPersistentRDDs ){
// 			println("PERSISTANT RDD NAME: "+rdd.name)
// 		  	if(rdd.name=="nRdd"){
// 		  		nInt=rdd.asInstanceOf[RDD[Int]].first()
// 		  		println("nInt var: "+nInt)
// 		  		println("nInt class: "+nInt.getClass)
// 		  		rdd.unpersist()
// 		  }
// 		}

// 	}
// 		for ((id: Int,rdd: org.apache.spark.rdd.RDD[_])<- sc.getPersistentRDDs ){
// 			println("PERSISTANT RDD NAME: "+rdd.name)
		 
// 		}
	

//   }

// }

// //   