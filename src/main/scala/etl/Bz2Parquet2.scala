package etl

import com.Log
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @BelongsProject: dmp
  * @BelongsPackage: etl
  * @Author: Flourish Sang
  * @CreateTime: 2019-02-27 16:39
  * @Description: ${Description}
  */
object Bz2Parquet2 {
  def main(args: Array[String]): Unit = {
    //模拟企业级编程
    if (args.length!=2){
      println("目录不正确，退出程序")
      sys.exit()
    }
    val Array(inputPath,outputPath) = args
    val conf = new SparkConf().setAppName(s"${this.getClass.getName}").setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val fields = spark.sparkContext.textFile(inputPath)
    val row = fields.map(t=>{
      t.split(",",t.length)
    }).filter(_.length>=85)
      .map(Log(_))

    val df = spark.createDataFrame(row)

    df.write.partitionBy("provincename","cityname").parquet(outputPath)

    spark.stop()
  }
}
