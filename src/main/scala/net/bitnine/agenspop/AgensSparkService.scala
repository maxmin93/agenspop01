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

      test
        .flatMap{ line =>  line.split(" ") }
        .map{ word => (word, 1) }
        .reduceByKey(_ + _)
        .saveAsTextFile("target/output.txt")
  }

}