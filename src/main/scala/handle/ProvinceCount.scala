package handle

import java.sql.DriverManager
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.JdbcMysql
import utils.JdbcMysql.load

import scala.util.Properties

/**
  * @BelongsProject: dmp
  * @BelongsPackage: handle
  * @Author: Flourish Sang
  * @CreateTime: 2019-03-01 09:59
  * @Description: ${3.2.0统计各省市数据量分布情况}
  */
object ProvinceCount {



  def main(args: Array[String]): Unit = {


//    val Array(inputPath,outputPath) = args
    val session = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val lines: DataFrame = session.read.parquet("hdfs://hadoopCDH:8020/user/dmp")
//
//    val words = lines.map(t=>{
//      val provincename = t.getAs("provincename")
//      val cityname = t.getAs("cityname")
//      ((provincename,cityname),1)
//    })

    lines.createTempView("t_WC")
    val sql0 = "select provincename,cityname,sum(1) as ct from t_WC group by provincename,cityname"

    val res0 = session.sql(sql0)
    res0.write.json("G:\\千峰\\资料\\spark项目\\data.txt")

//    JdbcMysql.getConn()


    val load = ConfigFactory.load()

    val url = "jdbc:mysql://hadoopCDH:3306/dmps?characterEncoding=utf-8"
    val conf = new Properties()
    conf.setProperty("user",load.getString("db.default.user"))
    conf.setProperty("password",load.getString("db.default.password"))

    res0.write.mode("append").jdbc(url,"province_counts",conf)

    val jsonStr = res0.toJSON.collectAsList().toString

    println(jsonStr)

    session.stop()
//    res0.show()

  }

}
