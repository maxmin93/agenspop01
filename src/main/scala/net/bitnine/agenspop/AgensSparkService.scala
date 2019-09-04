package net.bitnine.agenspop.service

import collection.JavaConverters._
import net.bitnine.agenspop.elasticgraph.ElasticGraphAPI
import net.bitnine.agenspop.elasticgraph.model.{ElasticEdge, ElasticVertex}
import org.apache.spark.SparkContext
import org.springframework.stereotype.Service
// import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Service
class AgensSparkService (@Autowired sc: SparkContext, baseAPI: ElasticGraphAPI) {

    val message: String = s"Hello scala~"
    type VertexId = String
    type EdgeId = String
/*
    // id, label, name(or datasource)
    val vertices: RDD[(VertexId, (String, String))]
    // id, source, target, label, datasource
    val edges: RDD[Edge[(String, String)]]
    // vertices, edges
    val graph: Graph[(String,String), (String,String)]

//    def addVertices(sliceJava : java.util.ArrayList[ElasticVertex]) = {
//
//        val slice : Iterable[ElasticVertex] = sliceJava.asScala
//        val vertices: RDD[(VertexId, (String, String))] = sc.parallelize(
//            List( slice.map{r => (r.getId, (r.getLabel, r.getDatasource)) }) )
//
//        this.vertices = this.vertices.union(vertices)
////            List(("3L", ("rxin", "student")), ("7L", ("jgonzal", "postdoc")),
////            ("5L", ("franklin", "prof")), ("2L", ("istoica", "prof"))))
//    }
*/

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