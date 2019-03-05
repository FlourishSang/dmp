package graph

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD

/**
  * @BelongsProject: dmp
  * @BelongsPackage: graph
  * @Author: Flourish Sang
  * @CreateTime: 2019-03-05 16:11
  * @Description: ${图计算例子}
  */
object CommonFirends {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getName}").setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    // 构造点的集合
    val vertexRDD: RDD[(Long, (String, Int))] = sc.makeRDD(Seq(
      (1L, ("老陈", 50)),
      (2L, ("老曹", 31)),
      (6L, ("老吴", 32)),
      (9L, ("智哥", 30)),
      (16L, ("大钊", 26)),
      (21L, ("亚东", 31)),
      (44L, ("侠姐", 38)),
      (5L, ("小马哥", 30)),
      (7L, ("小动", 48)),
      (133L, ("手机A", 1)),
      (138L, ("手机B", 2)),
      (158L, ("手机C", 1))
    ))

    // 构造边的集合
    val edgeRDD: RDD[Edge[Int]] = sc.makeRDD(Seq(
      Edge(1L, 133L, 0),
      Edge(2L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(9L, 133L, 0),
      Edge(6L, 138L, 0),
      Edge(21L, 138L, 0),
      Edge(16L, 138L, 0),
      Edge(44L, 138L, 0),
      Edge(5L, 158L, 0),
      Edge(7L, 158L, 0)
    ))
    //构建图计算的实例
    val graph = Graph(vertexRDD,edgeRDD)

    //图计算的原理就是取出其中一个最小的值，为顶点
    val vertices = graph.connectedComponents().vertices
    vertices.foreach(println)

    vertices.join(vertexRDD).map{
      case(userid,(cmid,(name,age))) => (cmid,List((name,age)))
    }.reduceByKey(_ ++ _).foreach(println)

    sc.stop()
  }

}
