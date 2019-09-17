package xia.v.lan

import java.util.logging.{Level, Logger}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author chenhao
  * @description <p>
  *              created by chenhao 2019/9/17 15:37
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARNING)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("scala-word-count").setMaster("local")
    val sc = new SparkContext(conf)
    val path = "file:///D:/temp/flink/全新安装.txt"
    val input = sc.textFile(path)
    val wc = input.flatMap(_.split(" ")).map((_,1)).reduceByKey((a, b) => a+b)
    wc.foreach(println)
    /**
      * //----因为没有hadoop，所以会报错，在linux下就不会有问题-------//
      * wc.saveAsTextFile("file:///D:/temp/flink/count.txt")
      */
    sc.stop()
  }

}
