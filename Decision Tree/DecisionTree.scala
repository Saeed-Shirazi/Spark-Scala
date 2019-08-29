package package1

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
import scala.math.sqrt
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators



  

/** Compute the average number of friends by age in a social network. */
import org.apache.spark.SparkConf
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.Gini

object DecisionTreeTest {
  
  
  def parsline (line : String) = {
      
      val fields = line.split(",")
      var one = fields(1).toInt
      var two = fields (3).toInt
      val three = fields (0).toDouble
    // val f = fields (3)
      //val three = fields(2).toString
      //val words = three.split( " " ).collect{case x if ( x.length>1) x(1)}
  //words.map( word => ( word, title ) )
      (one , two , three  )
    }
  def parsline2 (line : String) = {
      
      val fields = line.split(",")
      var one = fields(0).toString
      var two = fields (1).toString
      val three = fields (2).toString
    // val f = fields (3)
      //val three = fields(2).toString
      //val words = three.split( " " ).collect{case x if ( x.length>1) x(1)}
  //words.map( word => ( word, title ) )
      (one , two , three  )
    }
     def loadMovieNames() : Map[(String, String) , String] = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[(String, String) , String] = Map().withDefaultValue("0.0")
  
    
     val lines = Source.fromFile("C:/Users/S.Sikoor/Desktop/payan nameh/finalcom7/FINAALNOTEAdamic.csv/sorted/7.csv").getLines()
     for (line <- lines) {
       var fields = line.split(',')
       if (fields.length > 1) {
        movieNames += ((fields(1).toString.toLowerCase(),fields(3).toString.toLowerCase())->fields(0) )
       }
     }
    
     return movieNames
  }
  
          def loadMovieNames2() : Map[(String, String) , String] = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[(String, String) , String] = Map().withDefaultValue("0.0")
    
     val lines = Source.fromFile("C:/Users/S.Sikoor/Desktop/payan nameh/finalcom7/FINAALNOTEJaccard.csv/sorted/7.csv").getLines()
     for (line <- lines) {
       var fields = line.split(',')
       if (fields.length > 1) {
        movieNames += ((fields(1).toString.toLowerCase(),fields(3).toString.toLowerCase())->fields(0) )
       }
     }
    
     return movieNames
  }
  
  
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KMeansClustering")
    val sc = new SparkContext(sparkConf)


 sc.setLogLevel("WARN")
 
 /////making Input File for Decision Tree
// // according to labels data
//// val edges1 = sc.textFile("C:/Users/S.Sikoor/Desktop/payan nameh/finalcom5/FINAALNOTEAdamic.csv/sorted/5aa.csv")
//// val edges2 = sc.textFile("C:/Users/S.Sikoor/Desktop/payan nameh/finalcom5/FINAALNOTEJaccard.csv/sorted/5ja.csv")
//  val edges3 = sc.textFile("C:/Users/S.Sikoor/Desktop/Data For validation/Final Data  For Model/Zanan Khoon ofooni.csv")
//  
//// val yourRdd1 = edges1.map(parsline)
//// val yourRdd2 = edges2.map(parsline)
// val yourRdd3 = edges3.map(parsline2)
// val dict1 = loadMovieNames()
// 
// val dict2 = loadMovieNames2()
// 
// val res = yourRdd3.map(x=>( dict1(x._1 , x._2) , dict2(x._1 , x._2) , x._3 ))
// for (a<- res.collect)
//   println(a)
// 
//   val output = res.map(x=> (x._3+" " + "1:"+ x._1+ " " + "2:" + x._2  , x._1))
//   
//
//   import org.apache.spark.sql.SQLContext
//val sqlContext = new SQLContext(sc)
//   
//val df = sqlContext.createDataFrame(output)
//
//
//df.coalesce(1).write.format("com.databricks.spark.csv").mode("overwrite").save("C:/Users/S.Sikoor/Desktop/Data For validation/Final Data  For Model/Zanan Khoon ofooniInput.csv")
// /////////////////////  
   
 
 //////////Making Decision Tree Model
val data = MLUtils.loadLibSVMFile(sc, "C:/Users/S.Sikoor/Desktop/Data For validation/Final Data  For Model/Zanan Khoon ofooniInput.csv/a.txt")
// Split the data into training and test sets (30% held out for testing)
val splits = data.randomSplit(Array(0.7, 0.3))
val (trainingData, testData) = (splits(0), splits(1))

// Train a DecisionTree model.
//  Empty categoricalFeaturesInfo indicates all features are continuous.
val numClasses = 2
val categoricalFeaturesInfo = Map[Int, Int]()
val impurity = "gini"
val maxDepth = 5
val maxBins = 32

val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
  impurity, maxDepth, maxBins)

// Evaluate model on test instances and compute test error
val labelAndPreds = testData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
println("Test Error = " + testErr)
println("Learned classification tree model:\n" + model.toDebugString)

// Save and load model
model.save(sc, "target/tmp/myDecisionTreeClassificationModel")

  }
} 