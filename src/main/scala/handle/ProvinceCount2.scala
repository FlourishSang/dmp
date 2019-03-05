//package handle
//
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.{DataFrame, Row, SparkSession}
//
///**
//  * @BelongsProject: dmp
//  * @BelongsPackage: handle
//  * @Author: Flourish Sang
//  * @CreateTime: 2019-03-01 14:38
//  * @Description: ${Description}
//  */
//object ProvinceCount2 {
//  def main(args: Array[String]): Unit = {
//
//    val session = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
//
//    val lines= session.read.parquet("hdfs://hadoopCDH:8020/user/dmp")
//
//    val fields = lines.rdd.map(t=>{
//      val provincename = t.getAs("provincename")
//      val cityname = t.getAs("cityname")
//      ((provincename,cityname),1)
//    })
//    println(fields.collect())
//
//}
//
//
//}
