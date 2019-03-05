package handle


import java.util.Properties
import com.typesafe.config.ConfigFactory
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @BelongsProject: dmp
  * @BelongsPackage: handle
  * @Author: Flourish Sang
  * @CreateTime: 2019-03-01 19:01
  * @Description: ${Description}
  */
object AreaCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.debug.maxToStringFields","100")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val session = SparkSession.builder().appName(this.getClass.getName)
      .master("local[*]")
      .config(conf)
      .getOrCreate()

    val lines = session.read.parquet("hdfs://hadoopCDH:8020/user/dmp")

    lines.createOrReplaceTempView("t_msg")


    val sql0 = "select * ," +
      "(round(bidwin_count/bid_count,4)*100) as binwin_rate," +
      "(round(click_count/show_count,4)*100) as click_rate " +
      "from (" +
      "select provincename,cityname," +
      "sum(case when requestmode =1 and processnode>=1 then 1 else 0 end)as origin_count," +
      "sum(case when requestmode =1 and processnode>=2 then 1 else 0 end)as eff_count," +
      "sum(case when requestmode =1 and processnode=3 then 1 else 0 end)as ad_count," +
      "sum(case when ISEFFECTIVE =1 and ISBILLING=1 and ISBID=1 then 1 else 0 end)as bid_count, " +
      "sum(case when ISEFFECTIVE =1 and ISBILLING=1 and ISwin=1 and ADORDERID !=0 then 1 else 0 end)as bidwin_count," +
      "sum(case when REQUESTMODE=2 and ISEFFECTIVE =1 then 1 else 0 end)as show_count," +
      "sum(case when REQUESTMODE=3 and ISEFFECTIVE =1 then 1 else 0 end)as click_count," +
      "(sum(case when ISEFFECTIVE =1 and ISBILLING=1 and ISBID=1 then winprice else 0 end)/1000) as adcost_count," +
      "(sum(case when ISEFFECTIVE =1 and ISBILLING=1 and ISBID=1 then adpayment else 0 end)/1000) as consume_count " +
      "from t_msg " +
      "group by provincename,cityname )tmp "

    //val res0 = session.sql(sql0)
    //res0.show()

    val load = ConfigFactory.load()

    val properties = new Properties()
    properties.setProperty("user",load.getString("db.default.user"))
    properties.setProperty("password",load.getString("db.default.password"))
    val url = load.getString("db.default.url")

    //res0.write.mode("overwrite").jdbc(url,"area_dis",properties)

    val sql1 = "select * ," +
      "(round(bidwin_count/bid_count,4)*100) as binwin_rate," +
      "(round(click_count/show_count,4)*100) as click_rate " +
      "from (" +
      "select ispname," +
      "sum(case when requestmode =1 and processnode>=1 then 1 else 0 end)as origin_count," +
      "sum(case when requestmode =1 and processnode>=2 then 1 else 0 end)as eff_count," +
      "sum(case when requestmode =1 and processnode=3 then 1 else 0 end)as ad_count," +
      "sum(case when ISEFFECTIVE =1 and ISBILLING=1 and ISBID=1 then 1 else 0 end)as bid_count, " +
      "sum(case when ISEFFECTIVE =1 and ISBILLING=1 and ISwin=1 and ADORDERID !=0 then 1 else 0 end)as bidwin_count," +
      "sum(case when REQUESTMODE=2 and ISEFFECTIVE =1 then 1 else 0 end)as show_count," +
      "sum(case when REQUESTMODE=3 and ISEFFECTIVE =1 then 1 else 0 end)as click_count," +
      "(sum(case when ISEFFECTIVE =1 and ISBILLING=1 and ISBID=1 then winprice else 0 end)/1000) as adcost_count," +
      "(sum(case when ISEFFECTIVE =1 and ISBILLING=1 and ISBID=1 then adpayment else 0 end)/1000) as consume_count " +
      "from t_msg " +
      "group by ispname )tmp "

//    val res1 = session.sql(sql1)
//    res1.show()
//
//    res1.write.mode("overwrite").jdbc(url,"operation",properties)


    val sql2 = "select * ," +
      "(round(bidwin_count/bid_count,4)*100) as binwin_rate," +
      "(round(click_count/show_count,4)*100) as click_rate " +
      "from (" +
      "select networkmannername," +
      "sum(case when requestmode =1 and processnode>=1 then 1 else 0 end)as origin_count," +
      "sum(case when requestmode =1 and processnode>=2 then 1 else 0 end)as eff_count," +
      "sum(case when requestmode =1 and processnode=3 then 1 else 0 end)as ad_count," +
      "sum(case when ISEFFECTIVE =1 and ISBILLING=1 and ISBID=1 then 1 else 0 end)as bid_count, " +
      "sum(case when ISEFFECTIVE =1 and ISBILLING=1 and ISwin=1 and ADORDERID !=0 then 1 else 0 end)as bidwin_count," +
      "sum(case when REQUESTMODE=2 and ISEFFECTIVE =1 then 1 else 0 end)as show_count," +
      "sum(case when REQUESTMODE=3 and ISEFFECTIVE =1 then 1 else 0 end)as click_count," +
      "(sum(case when ISEFFECTIVE =1 and ISBILLING=1 and ISBID=1 then winprice else 0 end)/1000) as adcost_count," +
      "(sum(case when ISEFFECTIVE =1 and ISBILLING=1 and ISBID=1 then adpayment else 0 end)/1000) as consume_count " +
      "from t_msg " +
      "group by networkmannername )tmp "

//    val res2 = session.sql(sql2)
//    res2.show()
//
//    res2.write.mode("overwrite").jdbc(url,"network_name",properties)

    val sql3 = "select * ," +
      "(round(bidwin_count/bid_count,4)*100) as binwin_rate," +
      "(round(click_count/show_count,4)*100) as click_rate " +
      "from (" +
      "select devicetype," +
      "sum(case when requestmode =1 and processnode>=1 then 1 else 0 end)as origin_count," +
      "sum(case when requestmode =1 and processnode>=2 then 1 else 0 end)as eff_count," +
      "sum(case when requestmode =1 and processnode=3 then 1 else 0 end)as ad_count," +
      "sum(case when ISEFFECTIVE =1 and ISBILLING=1 and ISBID=1 then 1 else 0 end)as bid_count, " +
      "sum(case when ISEFFECTIVE =1 and ISBILLING=1 and ISwin=1 and ADORDERID !=0 then 1 else 0 end)as bidwin_count," +
      "sum(case when REQUESTMODE=2 and ISEFFECTIVE =1 then 1 else 0 end)as show_count," +
      "sum(case when REQUESTMODE=3 and ISEFFECTIVE =1 then 1 else 0 end)as click_count," +
      "(sum(case when ISEFFECTIVE =1 and ISBILLING=1 and ISBID=1 then winprice else 0 end)/1000) as adcost_count," +
      "(sum(case when ISEFFECTIVE =1 and ISBILLING=1 and ISBID=1 then adpayment else 0 end)/1000) as consume_count " +
      "from t_msg " +
      "group by devicetype )tmp "

//    val res3 = session.sql(sql3)
//    res3.show()
//
//    res3.write.mode("overwrite").jdbc(url,"device_type",properties)

    val sql4 = "select * ," +
      "(round(bidwin_count/bid_count,4)*100) as binwin_rate," +
      "(round(click_count/show_count,4)*100) as click_rate " +
      "from (" +
      "select client," +
      "sum(case when requestmode =1 and processnode>=1 then 1 else 0 end)as origin_count," +
      "sum(case when requestmode =1 and processnode>=2 then 1 else 0 end)as eff_count," +
      "sum(case when requestmode =1 and processnode=3 then 1 else 0 end)as ad_count," +
      "sum(case when ISEFFECTIVE =1 and ISBILLING=1 and ISBID=1 then 1 else 0 end)as bid_count, " +
      "sum(case when ISEFFECTIVE =1 and ISBILLING=1 and ISwin=1 and ADORDERID !=0 then 1 else 0 end)as bidwin_count," +
      "sum(case when REQUESTMODE=2 and ISEFFECTIVE =1 then 1 else 0 end)as show_count," +
      "sum(case when REQUESTMODE=3 and ISEFFECTIVE =1 then 1 else 0 end)as click_count," +
      "(sum(case when ISEFFECTIVE =1 and ISBILLING=1 and ISBID=1 then winprice else 0 end)/1000) as adcost_count," +
      "(sum(case when ISEFFECTIVE =1 and ISBILLING=1 and ISBID=1 then adpayment else 0 end)/1000) as consume_count " +
      "from t_msg " +
      "group by client )tmp "

//    val res4 = session.sql(sql4)
//    res4.show()

//    res4.write.mode("overwrite").jdbc(url,"client_type",properties)

    val sql5 = "select * ," +
      "(round(bidwin_count/bid_count,4)*100) as binwin_rate," +
      "(round(click_count/show_count,4)*100) as click_rate " +
      "from (" +
      "select appname," +
      "sum(case when requestmode =1 and processnode>=1 then 1 else 0 end)as origin_count," +
      "sum(case when requestmode =1 and processnode>=2 then 1 else 0 end)as eff_count," +
      "sum(case when requestmode =1 and processnode=3 then 1 else 0 end)as ad_count," +
      "sum(case when ISEFFECTIVE =1 and ISBILLING=1 and ISBID=1 then 1 else 0 end)as bid_count, " +
      "sum(case when ISEFFECTIVE =1 and ISBILLING=1 and ISwin=1 and ADORDERID !=0 then 1 else 0 end)as bidwin_count," +
      "sum(case when REQUESTMODE=2 and ISEFFECTIVE =1 then 1 else 0 end)as show_count," +
      "sum(case when REQUESTMODE=3 and ISEFFECTIVE =1 then 1 else 0 end)as click_count," +
      "(sum(case when ISEFFECTIVE =1 and ISBILLING=1 and ISBID=1 then winprice else 0 end)/1000) as adcost_count," +
      "(sum(case when ISEFFECTIVE =1 and ISBILLING=1 and ISBID=1 then adpayment else 0 end)/1000) as consume_count " +
      "from t_msg " +
      "group by appname )tmp "

    val res5 = session.sql(sql5)
    res5.show()

    res5.write.mode("overwrite").jdbc(url,"appname",properties)

    val sql6 = "select * ," +
      "(round(bidwin_count/bid_count,4)*100) as binwin_rate," +
      "(round(click_count/show_count,4)*100) as click_rate " +
      "from (" +
      "select adplatformkey," +
      "sum(case when requestmode =1 and processnode>=1 then 1 else 0 end)as origin_count," +
      "sum(case when requestmode =1 and processnode>=2 then 1 else 0 end)as eff_count," +
      "sum(case when requestmode =1 and processnode=3 then 1 else 0 end)as ad_count," +
      "sum(case when ISEFFECTIVE =1 and ISBILLING=1 and ISBID=1 then 1 else 0 end)as bid_count, " +
      "sum(case when ISEFFECTIVE =1 and ISBILLING=1 and ISwin=1 and ADORDERID !=0 then 1 else 0 end)as bidwin_count," +
      "sum(case when REQUESTMODE=2 and ISEFFECTIVE =1 then 1 else 0 end)as show_count," +
      "sum(case when REQUESTMODE=3 and ISEFFECTIVE =1 then 1 else 0 end)as click_count," +
      "(sum(case when ISEFFECTIVE =1 and ISBILLING=1 and ISBID=1 then winprice else 0 end)/1000) as adcost_count," +
      "(sum(case when ISEFFECTIVE =1 and ISBILLING=1 and ISBID=1 then adpayment else 0 end)/1000) as consume_count " +
      "from t_msg " +
      "group by adplatformkey )tmp "

    val res6 = session.sql(sql6)
    res6.show()

    res6.write.mode("overwrite").jdbc(url,"adplatformkey",properties)

    session.stop()

  }

}
