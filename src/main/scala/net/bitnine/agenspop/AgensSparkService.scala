package net.bitnine.agenspop.service

import org.apache.spark.SparkContext
import org.springframework.stereotype.Service
// import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Service
class AgensSparkService (@Autowired sc: SparkContext) {

  val message: String = s"Hello scala~"

  def hello: String = {
    s"${message}"
  }

  def wordcount = {
      val test = sc.textFile("README.md")

      test.flatMap { line =>  //for each line
        line.split(" ") //split the line in word by word.
      }
      .map { word =>        //for each word
        (word, 1)           //Return a key/value tuple, with the word as key and 1 as value
      }
      .reduceByKey(_ + _) //Sum all of the value with same key
      .saveAsTextFile("target/output.txt") //Save to a text file
  }

}