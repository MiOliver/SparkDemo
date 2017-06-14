package com.ning.common;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by oliver on 2017/6/14.
 */
public class SparkTest implements Serializable {

    private static final long serialVersionUID = 1L;

    public void deal(){
        System.out.println("start count");
        SparkConf conf=new SparkConf().setMaster("local").setAppName("SparkTest");
        JavaSparkContext sc= new JavaSparkContext(conf);
        /**读入数据*/
        JavaRDD<String> input= sc.textFile("/Users/oliver/tem/test.txt");
        JavaRDD<String> words=input.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        /**转换为键值对&技术*/
        JavaPairRDD<String,Integer> counts = words.mapToPair(
                new PairFunction<String, String,Integer>() {
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2(s,1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        counts.saveAsTextFile("/Users/oliver/tem/testResult");
        System.out.println("end test");
    }

}
