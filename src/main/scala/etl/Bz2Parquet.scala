package etl

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import utils.{NBF, SchemaUtils}

/**
  * @BelongsProject: dmp
  * @BelongsPackage: etl
  * @Author: Flourish Sang
  * @CreateTime: 2019-02-27 15:21
  * @Description: ${处理数据，进行etl}
  */
object Bz2Parquet {
  def main(args: Array[String]): Unit = {
    //模拟企业级编程，首先判断目录是否为空
    if (args.length != 2){
      println("目录不正确，退出程序")
      sys.exit()
    }
    //创建一个集群存储输入输出的目录
    val Array(inputPath,outputPath) = args

    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val fields = spark.sparkContext.textFile(inputPath)

    val row_data = fields.map(t =>
      t .split(","))
      .filter(_.length >= 85)
      .map(arr=>{
      Row(
        arr(0),
        NBF.toInt(arr(1)),
        NBF.toInt(arr(2)),
        NBF.toInt(arr(3)),
        NBF.toInt(arr(4)),
        arr(5),
        arr(6),
        NBF.toInt(arr(7)),
        NBF.toInt(arr(8)),
        NBF.toDouble(arr(9)),
        NBF.toDouble(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        NBF.toInt(arr(17)),
        arr(18),
        arr(19),
        NBF.toInt(arr(20)),
        NBF.toInt(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        NBF.toInt(arr(26)),
        arr(27),
        NBF.toInt(arr(28)),
        arr(29),
        NBF.toInt(arr(30)),
        NBF.toInt(arr(31)),
        NBF.toInt(arr(32)),
        arr(33),
        NBF.toInt(arr(34)),
        NBF.toInt(arr(35)),
        NBF.toInt(arr(36)),
        arr(37),
        NBF.toInt(arr(38)),
        NBF.toInt(arr(39)),
        NBF.toDouble(arr(40)),
        NBF.toDouble(arr(41)),
        NBF.toInt(arr(42)),
        arr(43),
        NBF.toDouble(arr(44)),
        NBF.toDouble(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        NBF.toInt(arr(57)),
        NBF.toDouble(arr(58)),
        NBF.toInt(arr(59)),
        NBF.toInt(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        NBF.toInt(arr(73)),
        NBF.toDouble(arr(74)),
        NBF.toDouble(arr(75)),
        NBF.toDouble(arr(76)),
        NBF.toDouble(arr(77)),
        NBF.toDouble(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        NBF.toInt(arr(84))
      )

    })
    val df = spark.createDataFrame(row_data, SchemaUtils.schema)

    df.coalesce(1).write.parquet(outputPath)
    spark.stop()

  }
}
