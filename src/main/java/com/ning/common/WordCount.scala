package com.ning.common

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by oliver on 2017/6/17.
  */
class WordCount {

    def run() {
        val outputFile="hdfs://localhost:9000/oliver/temResult.txt"
        val conf=new SparkConf().setAppName("wordCount").setMaster("local")
        val sc=new SparkContext(conf)
        val input =sc.textFile("hdfs://localhost:9000/oliver/tem")
        val words=input.flatMap(line=>line.split(" "))
        val counts= words.map(words=>(words,1)).reduceByKey{case (x,y) =>x+y}
        counts.saveAsTextFile(outputFile)
    }
}
