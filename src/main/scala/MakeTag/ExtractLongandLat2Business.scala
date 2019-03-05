package MakeTag

import ch.hsr.geohash.GeoHash
import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import utils.{BaiduLBSHandler, JedisConnectionPool}

/**
  * @BelongsProject: dmp
  * @BelongsPackage: MakeTag
  * @Author: Flourish Sang
  * @CreateTime: 2019-03-04 19:33
  * @Description: ${生成商圈标签存储到Redis}
  */
object ExtractLongandLat2Business {
  def main(args: Array[String]): Unit = {
    if (args.length != 1){
      println("argument is wrong!!!")
      sys.exit()
    }
    val Array(inputPath)=args
    val conf = new SparkConf().setAppName(this.getClass.getName)
      .setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val session = SparkSession.builder().config(conf).getOrCreate()

    //获取源氏数据中的经纬度信息
    val longAndLat = session.read.parquet(inputPath)
      .select("long","lat")
      .filter("cast(long as double) >= 73.66 and cast (long as double) <= 135.05 " +
        "and cast(lat as double) >= 3.86 and cast(lat as double)<=53.55")
      .distinct()

      //将商圈信息持久化到Redis中，以便于以后查找
    longAndLat.foreachPartition(t=>{
      val jedis = JedisConnectionPool.getConnection()
      t.foreach(t=>{
        println(t)
        val long = t.getAs[String]("long")
        val lat = t.getAs[String]("lat")
        //通过百度的逆地址解析，获取到商圈信息
        val geoHashs = GeoHash.geoHashStringWithCharacterPrecision(long.toDouble,lat.toDouble,8)
        //进行sn验证
        val business = BaiduLBSHandler.parseBusinessTagBy(long,lat)
        if (StringUtils.isNotBlank(business))
          jedis.set(geoHashs,business)
      })
      jedis.close()

    })
    // 测试
        longAndLat.rdd.map(t => {
          val long = t.getAs[String]("long")
          val lat = t.getAs[String]("lat")
          val geoHash = GeoHash.geoHashStringWithCharacterPrecision(lat.toDouble, long.toDouble, 8)
          val business = BaiduLBSHandler.parseBusinessTagBy(long, lat)
          (geoHash, business)
        }).foreach(t => println(t._1, t._2))
  }

}
