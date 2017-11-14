package com.tjlcast.Demo.WordCount

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tangjialiang on 2017/11/12.
  */
object WordCountRdd_scala {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[2]").setAppName("wordcountscals")
        val sc = new SparkContext(sparkConf)

        val path = "file:///Users/tangjialiang/IdeaProjects/MySparkJavaScala/out/artifacts/MySparkJavaScala_jar/data.txt";
        val minPartitions = 2 ;
        val data = sc.textFile(path, minPartitions)

        val counts = data.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)

        counts.cache()
        val getData = counts.collect()

        for(e <- getData) {
            println(e)
        }
    }
}

class WordCountRdd_scala {

}
