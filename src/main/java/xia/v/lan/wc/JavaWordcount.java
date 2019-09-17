package xia.v.lan.wc;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import scala.Function1;
import scala.Int;
import scala.Tuple2;
import scala.collection.TraversableOnce;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author chenhao
 * @description <p>
 * created by chenhao 2019/9/17 14:22
 */
public class JavaWordcount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("word-count").setMaster("local");
        JavaSparkContext  sc = new JavaSparkContext(conf);
        String filePath = "file:///D:/temp/flink/全新安装.txt";
        JavaRDD<String> input = sc.textFile(filePath, 2);
        JavaRDD<String> flatMap = input.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        JavaPairRDD<String, Integer> mapToPair = flatMap.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> reduceByKey = mapToPair.reduceByKey(new Function2<Integer, Integer, Integer>() {

            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        reduceByKey.foreach(new VoidFunction<Tuple2<String,Integer>>(){

            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                Thread.sleep(100);
                System.out.println(stringIntegerTuple2._1 + "---" + stringIntegerTuple2._2);
            }
        });
        sc.close();
    }
}
