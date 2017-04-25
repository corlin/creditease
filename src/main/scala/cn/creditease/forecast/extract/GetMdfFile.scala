package cn.creditease.forecast.extract

/**
  * Created by corlinchen on 2017/4/21.
  */

import com.alibaba.fastjson._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent



object GetMdfFile {

  def main(args: Array[String]): Unit = {
    var masterUrl = "local[1]"
    if (args.length > 0) {
      masterUrl = args(0)
    }
    val conf = new SparkConf().setMaster(masterUrl).setAppName("GetMdfFile")
    val sc = new SparkContext(conf)
    val rd=sc.textFile("/devbase/mdf500.log",2)
    val events = rd.foreach(line => {
      //println(s"Line ${line}.")
      val data=JSON.parseArray(line)
      Some(data)
      //println(data.size())
      for(i <- 0 until data.size()){
        val datai=JSON.parseArray(data.getString(i))
        //println()
        val time=JSON.parseObject(datai.getString(0)).getString("time")
        val frame =JSON.parseObject(datai.getString(0)).getString("frames")
        val frames = JSON.parseObject(frame)
        val servers = frames.getString("server")
        if(servers!= null ){
          val serverArray =JSON.parseArray(servers)
          //println(serverArray.size())
          for(j <- 0 until serverArray.size()){
            //            println(serverArray.get(j))
            val instances = JSON.parseObject(serverArray.getString(j)).getString("Instances")
            val meId = JSON.parseObject(serverArray.get(j).toString).getString("MEId")
            val instanceArray = JSON.parseArray(instances)//instances 里可能有多个value,因此转换为数组
            if(instanceArray.size()>0){
              for (k <- 0 until instanceArray.size()){
                val values = JSON.parseObject(instanceArray.getString(k)).getString("values")
                val id = JSON.parseObject(instanceArray.getString(k)).getString("id")
                println(time+","+meId+",\""+id+"\","+meIdMatchValue(meId,values))
              }
            }

          }
        }
        val clients =frames.getString("client")
        if(clients!=null){
          val clientArray =JSON.parseArray(clients)
          //println(clientArray.size())
          for(j <- 0 until clientArray.size()){
            //            println(serverArray.get(j))
            val instances = JSON.parseObject(clientArray.getString(j)).getString("Instances")
            val meId = JSON.parseObject(clientArray.get(j).toString).getString("MEId")
            val instanceArray = JSON.parseArray(instances)//instances 里可能有多个value,因此转换为数组
            if(instanceArray.size()>0  ){
              for (k <- 0 until instanceArray.size()){
                val values = JSON.parseObject(instanceArray.getString(k)).getString("values")
                val id = JSON.parseObject(instanceArray.getString(k)).getString("id")
                println(time+","+meId+","+"\""+id+"\""+", "+meIdMatchValue(meId,values))
              }
            }

          }
        }
        //println()

      }
    })
    /*
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    */
    sc.stop()
  }


  def meIdMatchValue(MEId:String,jsonString:String): String ={
    var str = ""
    MEId match {
      case "clientResp"  => str=clientRespValue(jsonString);
      case "urlResp"  => str=urlRespAndappRespAndserverRespValue(jsonString);
      case "appResp"  => str=urlRespAndappRespAndserverRespValue(jsonString);
      case "serverResp"  => str=urlRespAndappRespAndserverRespValue(jsonString);
      case "jvm"  => str=jvmValue(jsonString);
    }
    str
  }
  def clientRespValue(jsonString:String): String ={
    val json = JSON.parseObject(jsonString)
    val tmax = json.getString("tmax")
    val tmin = json.getString("tmin")
    val tavg = json.getString("tavg")
    val tsum = json.getString("tsum")
    val count = json.getString("count")
    val err = json.getString("err")
    val warn = json.getString("warn")
    val str = tmax+","+tmin+","+tavg+","+tsum+","+count+","+err+","+warn
    str
  }
  def urlRespAndappRespAndserverRespValue(jsonString:String): String ={
    val json = JSON.parseObject(jsonString)
    val tmax = json.getString("tmax")
    val tmin = json.getString("tmin")
    val tavg = json.getString("tavg")
    val tsum = json.getString("tsum")
    val count = json.getString("count")
    val err = json.getString("err")
    val warn = json.getString("warn")
    val rc = json.getString("RC")
    val str = tmax+","+tmin+","+tavg+","+tsum+","+count+","+err+","+warn+","+rc
    str
  }
  def jvmValue(jsonString:String): String ={
    val json = JSON.parseObject(jsonString)

    val cpu_s = json.getString("cpu_s")
    val cpu_p = json.getString("cpu_p")
    val thread_live = json.getString("thread_live")
    val thread_daemon = json.getString("thread_daemon")
    val thread_peak = json.getString("thread_peak")

    val thread_started = json.getString("thread_started")
    val class_load = json.getString("class_load")
    val class_unload = json.getString("class_unload")
    val class_total = json.getString("class_total")
    val mgc_count = json.getString("mgc_count")
    val mgc_time = json.getString("mgc_time")
    val fgc_count = json.getString("fgc_count")
    val fgc_time = json.getString("fgc_time")
    val heap_use = json.getString("heap_use")
    val heap_init = json.getString("heap_init")
    val heap_commit = json.getString("heap_commit")
    val heap_max = json.getString("heap_max")
    val noheap_use = json.getString("noheap_use")
    val noheap_init = json.getString("noheap_init")
    val noheap_commit = json.getString("noheap_commit")
    val noheap_max = json.getString("noheap_max")
    val perm_use = json.getString("perm_use")
    val perm_init = json.getString("perm_init")
    val perm_commit = json.getString("perm_commit")
    val perm_max = json.getString("perm_max")

    val code_use = json.getString("code_use")
    val code_init = json.getString("code_init")
    val code_commit = json.getString("code_commit")
    val code_max = json.getString("code_max")
    val eden_use = json.getString("eden_use")
    val eden_init = json.getString("eden_init")
    val eden_commit = json.getString("eden_commit")
    val eden_max = json.getString("eden_max")
    val surv_use = json.getString("surv_use")
    val surv_init = json.getString("surv_init")
    val surv_commit = json.getString("surv_commit")
    val surv_max = json.getString("surv_max")
    val old_use = json.getString("old_use")
    val old_init = json.getString("old_init")
    val old_commit = json.getString("old_commit")
    val old_max = json.getString("old_max")
    val str =cpu_s+","+cpu_p+","+cpu_p+","+thread_live+","+thread_daemon+","+thread_peak+","+thread_started+","+class_load+","+class_unload+","+class_total+mgc_count+","+
      mgc_time+","+fgc_count+","+fgc_time+","+heap_use+","+heap_init+","+heap_commit+","+heap_max+","+noheap_use+","+noheap_init+","+noheap_commit+","+noheap_max+","+perm_use+","+
      perm_init+","+perm_commit+","+perm_max+","+code_use+","+code_init+","+code_commit+","+code_max+","+eden_use+","+eden_init+","+eden_commit+","+eden_max+","+surv_use+","+surv_init+","+
      surv_commit+","+surv_max+","+old_use+","+old_init+","+old_commit+","+old_max
    str
  }
}
