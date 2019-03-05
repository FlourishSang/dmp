package MakeTag

import ch.hsr.geohash.GeoHash
import com.typesafe.config.ConfigFactory
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.{JedisConnectionPool, TagUtils}

/**
  * @BelongsProject: dmp
  * @BelongsPackage: MakeTag
  * @Author: Flourish Sang
  * @CreateTime: 2019-03-02 10:10
  * @Description: ${Description}
  */
object DataTag {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .set("spark.debug.maxToStringFields","100")
    val session = SparkSession.builder().config(conf).getOrCreate()
    val sc = session.sparkContext

//    //配置HBASE的基本信息
//    val load = ConfigFactory.load()
//    val hbaseTableName = load.getString("hbase.table.name")
//    //配置HBASE的连接
//    val configuration = sc.hadoopConfiguration
//    configuration.set("hbase.zookeeper.quorum",load.getString("hbase.zookeeper.host"))
//    val hbConn = ConnectionFactory.createConnection(configuration)
//    //获得操作对象
//    val hbadmin = hbConn.getAdmin
//    if (!hbadmin.tableExists(TableName.valueOf(hbaseTableName))){
//      println("这个表可用！！！")
//      //创建表对象
//      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
//      //创建一个列簇
//      val columnDescriptor = new HColumnDescriptor("tags")
//      //将列簇放入到表中
//      tableDescriptor.addFamily(columnDescriptor)
//      hbadmin.createTable(tableDescriptor)
//      hbadmin.close()
//      hbConn.close()
//    }
//    //创建job对象
//    val jobConf = new JobConf(configuration)
//    //指定输出类型
//    jobConf.setOutputFormat(classOf[TableOutputFormat])
//    //指定输出表
//    jobConf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)


    val lines: DataFrame = session.read.parquet("hdfs://hadoopCDH:8020/user/dmp")
    val app_dict = session.sparkContext.textFile("G:\\千峰\\资料\\spark项目\\dmp\\app_dict.txt")
      .map(_.split("\t",-1))
      .filter(_.length>=6)
      .map(arr=>{
        (arr(4),arr(1))
    }).collect().toMap
    val app_dictBroadcast = session.sparkContext.broadcast(app_dict)

    val stopwords = session.sparkContext.textFile("G:\\千峰\\资料\\spark项目\\dmp\\stopwords.txt").collect()

    val stopwordsBroadcast = session.sparkContext.broadcast(stopwords)

    //过滤需要的userID，因为userID很多，只需要过滤出userID不全为空的
    val lines1 = lines.filter(TagUtils.hasneedOneUserId)
    val fields = MakeAdTag(lines1,app_dictBroadcast,stopwordsBroadcast)
    fields.foreach(println)

    //进行聚合
    val aggrUserTags = fields.reduceByKey((list1,list2)=>{
      (list1:::list2).groupBy(_._1)
        .mapValues(_.foldLeft(0)(_+_._2))
        .toList
      })
    //测试，整理并输出数据
    aggrUserTags.map(t=>{
      t._1 + "," + t._2.map(t=>t._1 + ":" + t._2).mkString(",")
    }).saveAsTextFile("hdfs://hadoopCDH:8020/user/dmpout")

//    //写入到HBASE中
//    aggrUserTags.map{
//      case (userid,userTags)=>{
//        val put = new Put(Bytes.toBytes(userid))
//        val tags = userTags.map(t => t._1 + ":" + t._2)mkString(",")
//        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes("2019-3-5"),Bytes.toBytes(tags))
//
//        (new ImmutableBytesWritable(),put)
//      }
//    }.saveAsHadoopDataset(jobConf)

    session.stop()
  }

  def MakeAdTag(lines: DataFrame, app_dictBroadcast: Broadcast[Map[String, String]],stopwordsBroadcast:Broadcast[Array[String]])={


    val fields1=lines.rdd.mapPartitions(t=> {
      val jedis = JedisConnectionPool.getConnection()
      t.map(t => {
        //创建一个集合用于返回
        var list1 = List[(String, Int)]()

        val userid = TagUtils.getAnyOneUserId(t)


        val adspacetype = t.getAs[Int]("adspacetype")
        val adspacetypename = t.getAs[String]("adspacetypename")
        val appid = t.getAs[String]("appid")
        val appname = t.getAs[String]("appname")
        val adplatformproviderid = t.getAs[Int]("adplatformproviderid")


        if (adspacetype < 10) {
          list1 :+= ("LC0" + adspacetype.toString, 1)
        } else {
          list1 :+= ("LC" + adspacetype.toString, 1)
        }

        list1 :+= ("LN" + adspacetypename, 1)
        if (appname.isEmpty) {
          list1 :+= ("APP" + app_dictBroadcast.value.getOrElse(appid, appid), 1)

        } else {
          list1 :+= ("APP" + appname, 1)
        }
        list1 :+= ("CN" + adplatformproviderid, 1)
        //操作系统
        val client = t.getAs[Int]("client")
        client match {
          case 1 => list1 :+= ("D00010001", 1)
          case 2 => list1 :+= ("D00010002", 1)
          case 3 => list1 :+= ("D00010003", 1)
          case _ => list1 :+= ("D00010004", 1)
        }
        //设备联网方式
        val network = t.getAs[String]("networkmannername")
        network match {
          case "WIFI" => list1 :+= ("D00020001", 1)
          case "4G" => list1 :+= ("D00020002", 1)
          case "3G" => list1 :+= ("D00020003", 1)
          case "2G" => list1 :+= ("D00020004", 1)
          case _ => list1 :+= ("D00020005", 1)
        }
        // 移动 运营商
        val ispname = t.getAs[String]("ispname")
        ispname match {
          case "移动" => list1 :+= ("D00030001", 1)
          case "联通" => list1 :+= ("D00030002", 1)
          case "电信" => list1 :+= ("D00030003", 1)
          case _ => list1 :+= ("D00030004", 1)
        }
        //关键字标签
        val keywords = t.getAs[String]("keywords").split("\\|")
          .filter(word => {
            word.length >= 3 && word.length <= 8 && !stopwordsBroadcast.value.contains(word)
          }).foreach(word => {
          list1 :+= ("K" + word, 1)
        })
        //地域标签
        val province = t.getAs[String]("provincename")
        if (StringUtils.isNotBlank(province)) {
          list1 :+= ("ZP" + province, 1)
        }
        val city = t.getAs[String]("cityname")
        if (StringUtils.isNotBlank(city)) {
          list1 :+= ("ZC" + city, 1)
        }
        //商圈标签
        if (t.getAs[String]("long").toDouble >= 73.66 && t.getAs[String]("long").toDouble >= 135.05
          && t.getAs[String]("lat").toDouble >= 3.86 && t.getAs[String]("lat").toDouble <= 53.55) {
          val lat = t.getAs[String]("lat")
          val long = t.getAs[String]("long")
          val geoHash = GeoHash.geoHashStringWithCharacterPrecision(lat.toDouble, long.toDouble, 8)
          val business = jedis.get(geoHash)
          business.split(";").foreach(t => list1 :+= (t, 1))
        }

        (userid, list1)
      })
    })
    fields1

  }

}
