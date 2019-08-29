package package1
//package ml.sparkling.graph.examples

import org.apache.log4j.Logger
import org.apache.spark.graphx.Graph

import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.io._


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
//import com.databricks.spark.csv.util

import org.apache.spark.sql.SQLContext
// For implicit conversions from RDDs to DataFrames
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import scala.math.log
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
object LastTry {
  
  def loadMovieNames() : Map[String, String] = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[  String, String] = Map()
    
     val lines = Source.fromFile("../nodeDegreesNOTE.csv").getLines()
      for (line <- lines) {
       var fields = line.split(',')
       if (fields.length > 1) {
        movieNames += (fields(0) -> fields(1))
       }
     }
    return movieNames
  
    }
  def parsline (line : String) = {
      
      val fields = line.split(",")
      var one = fields(0).toLong
      var two = fields (1).toLong
      
     // var six= fields (5).toString
      //val three = fields(2).toString
      //val words = three.split( " " ).collect{case x if ( x.length>1) x(1)}
  //words.map( word => ( word, title ) )
      (one , two     )
    }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("AdamicAdar").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
import org.apache.spark.sql.SQLContext
val sqlContext = new SQLContext(sc)


import sqlContext.implicits._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, LongType}

    val edges = sc.textFile("../KatzDataTest.csv")
    val newRdd = edges.map(parsline)
    ///////////////////////////NodesDegreeCount
   // var Docslvl1:Map[  Int, Long] = Map()
    //var Drugslvl1:Map[  Int, Long] = Map()
    val docs = newRdd.map(x=> ((x._1), 1)).reduceByKey((x , y) => x+y)
    //val Docslvl1 = docs.collectAsMap
    //val Dict1 = docs.foreach(x=> Docslvl1 += (x._1.toInt -> x._2))
    val drugs = newRdd.map(x=> ((x._2), 1)).reduceByKey((x , y) => x+y)
    //val Drugslvl1 = drugs.collectAsMap
   // val dict2 = drugs.foreach(x=> Drugslvl1 += (x._1.toInt -> x._2))
    val allnodes = docs.union(drugs)
    val allNodesDegreeMap = allnodes.collectAsMap
    // println( Docslvl1(6))
 ////////////////////////////////////////////   

     var dd = newRdd.map(x=> (x._1 , x._2)) 
    val ss = newRdd.map(x=> (x._2 , x._1 ))
    val df1 = sqlContext.createDataFrame(ss).toDF( "name1" , "name2")
   val df2 = sqlContext.createDataFrame(dd).toDF( "name3" ,  "name4")
    
       val dff = df2.join(df1, df2.col("name4") === df1.col("name1"))
   val ffr2 =dff.filter(x=> x(0)!=x(3)  ).toDF("name1","name2","name3","name4")
   val ff2 = ffr2.map(x=> (x(0).toString() , x(3).toString)).distinct()
   val df7 = ff2.map(x=> ((x._1.toLong) , 1))
   val fff2 = df7.rdd
   val fromDoc = fff2.reduceByKey((x , y) => x+y)
   val fromDocMap = fromDoc.collectAsMap()
       val df4 = ffr2.join(df2, df2.col("name3") === ffr2.col("name4"))
   val ff8 =df4.filter(x=> x(1)!=x(5)  ).toDF("name1","name2","name3","name4" , "name5" , "name6")
   //val ff3 = ff2.map(x=> (x(0) , x(1) , x(3) , x(5)))
   val ff3 = ff8.drop("name2").drop("name4")
   val o = ff3.rdd
   
    
    //////////////////Adamic-Adar

     

   val t= o.map(x=> (x(0).toString() , x(3).toString , allNodesDegreeMap(x(1).toString().toLong) , allNodesDegreeMap(x(2).toString().toLong))) 
   val r = t.map(x=>  ((x._1.toLong , x._2.toLong) , 1/log(x._3.toLong).toDouble + 1/log(x._4.toLong).toDouble))
   val finalRes = r.reduceByKey((x,y)=> x+y)
   val finalRes2 = finalRes.map(x=> ((x._1._1 , x._1._2 ), x._2))
   val tt = finalRes2.map(x=>(x._1._1 , x._1._2, x._2))
  val uuu = sqlContext.createDataFrame(tt)
uuu.coalesce(1).write.format("com.databricks.spark.csv").mode("overwrite").save("D:/FINAALNOTEAdamicAll.csv")
           val finalResAdamic = finalRes2.collectAsMap()
   val FinaalAdamic = newRdd.map(x=> (x._1 , x._2 ,finalResAdamic.getOrElse((x._1.toLong , x._2.toLong), default = 0).toString().toDouble)) 
    val uu = sqlContext.createDataFrame(FinaalAdamic)
//   for(s<- dgFinaal.collect)
//     println("finalJac" +s)
    uu.coalesce(1).write.format("com.databricks.spark.csv").mode("overwrite").save("D:/FINAALNOTEAdamic.csv")
//      for(a<- FinaalAdamic)
//     println("final adamic " +a)
  
    /////////////////////Jaccard Link Prediction
       
    
       
   val df = df1.join(df2, df2.col("name3") === df1.col("name2"))
   val ffr =df.filter(x=> x(0)!=x(3)  ).toDF("name1","name2","name3","name4")
   val ff = ffr.map(x=> (x(0).toString() , x(3).toString))
   val df3 = ff.map(x=> ((x._1.toLong) , 1))
   val fff = df3.rdd
   val fromDrug = fff.reduceByKey((x , y) => x+y)
   val fromDrugMap = fromDrug.collectAsMap()
//   for(a<- fromDrug.collect)
//     println(a)
   

  val NumberOfNeighborNodes1 = o.map(x=>((x(0).toString() , x(3).toString), x(1))).distinct()
  val NumberOfNeighborNodes2 = o.map(x=>((x(0).toString() , x(3).toString), x(2))).distinct()

  val NumberOfNeighborNodes = NumberOfNeighborNodes1.union(NumberOfNeighborNodes2)
  val temp =NumberOfNeighborNodes.map(x=>((x._1) , 1)) 
  val NumberOfNeighborNodesF = temp.reduceByKey((x , y) => x+y)

   val finalresJaccard = NumberOfNeighborNodesF.map(x=> ((x._1._1.toLong , x._1._2.toLong) , (x._2.toDouble/(fromDocMap.getOrElse(x._1._1.toLong , default = 0)+fromDrugMap.getOrElse(x._1._2.toLong , default = 0) -2 ).toDouble)))

   
   
    val finalResJac = finalresJaccard.collectAsMap()
   val dgFinaal = newRdd.map(x=> (x._1 , x._2 ,finalResJac.getOrElse((x._1.toLong , x._2.toLong), default = 0).toString().toDouble))
   val u = sqlContext.createDataFrame(dgFinaal)
//   for(s<- dgFinaal.collect)
//     println("finalJac" +s)
    u.coalesce(1).write.format("com.databricks.spark.csv").mode("overwrite").save("D:/FINAALNOTEJaccard.csv")

    ////////////////////////////////////////////////////////////
   

   //////////////////////////Katz
   
   //////////////number of path length 5

//   val path5 = ff3.join(ffr, ff3.col("name6") === ffr.col("name1"))
//    
//   val ff5 =path5.filter(x=> x(2)!=x(6)&& (x(6),x(7))!=(x(0),x(1))   ).toDF("name1","name2","name3","name4" , "name5" , "name6" , "name7" , "name8" )
//   val ff6 = ff5.drop("name4").drop("name6")
//   
//  val pathl5 = ff6.map(x=> ((x(0).toString() , x(5).toString()) , 1))
//     
//  val temp2 = pathl5.rdd
//  val temp3 = temp2.reduceByKey((x , y)=> x+y)
//  val pathl5Final = temp3.map(x=> ((x._1._1 , x._1._2) , x._2*0.2*0.2))
////  for(s<- pathl5Final.collect)
////     println("path5" +s)
// println(temp3.count())
// /////////
// val pathl3 = ff3.map(x=> ((x(0).toString() , x(3).toString()) , 1))
//     
//  val temp4 = pathl3.rdd
//  val temp5 = temp4.reduceByKey((x , y)=> x+y)
//  val pathl3Final = temp5.map(x=> ((x._1._1 , x._1._2) , x._2*0.2))
////for(s<- pathl3Final.collect)
////     println("path3" +s)
//// println(temp5.count())
//  val katz = pathl5Final.union(pathl3Final)
////  for(s<- katz.collect)
////    println("katz" +s)
//    
//  val finalKatz = katz.reduceByKey((x , y) => x+y).map(x=> (x._1._1 , x._1._2 , x._2))
//     val uuu = sqlContext.createDataFrame(finalKatz)
////   for(s<- dgFinaal.collect)
////     println("finalJac" +s)
//    uuu.coalesce(1).write.format("com.databricks.spark.csv").mode("overwrite").save("D:/FINAALNOTEKatz.csv")
//  
//  for(s<- finalKatz.collect)
//    println("katz" +s)

   ////////////////////////////////

   
  }
}

