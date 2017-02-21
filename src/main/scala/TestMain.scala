// package main.scala

// import org.apache.spark.SparkConf
// import org.apache.spark.SparkContext
// import org.apache.spark.SparkContext._
// import collection.JavaConversions._
// import org.apache.spark.rdd.RDD


// object TestMain {
  
//   def main(args: Array[String]) {
// //    println(this.getClass().getCanonicalName())
//     val conf = new SparkConf().setMaster("local").setAppName("My App")
//     val sc = new SparkContext(conf)
//     var arr=sc.parallelize(List((1.1,1.5),(1.1,2.2),(1.1,3.7),(1.1,4.2),(1.1,5.9),(1.1,6.6),(1.1,7.4),(1.1,8.9),(1.1,9.8)))
//     val arr2 = arr.map(values=>(values._1,(values._1,values._2)))
  
//     arr2.collect().foreach(println)
    
//     val sumCount = arr2.combineByKey((v) => (v._2.toDouble / v._1, 1),(acc: (Double, Int), q:((Double,Double))) => ((q._2/q._1)+acc._1,acc._2+1),(acc1: (Double, Int), acc2: (Double, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
//       //println(sumCount.first())
//     sumCount.collect().foreach(println)
//     sc.stop()
// //    
//   }
// //  def getLocalReachDistance(sc:SparkContext):RDD[Long]={
// //    var arr=sc.parallelize(List((1,1),(1,2),(1,3),(1,4),(1,5),(1,6),(1,7),(1,8),(1,9)))
// //    val arr2 = arr.map(values=>(values._1,(values._1,values._2)))
// //    arr2
// //  }
// }

// //   