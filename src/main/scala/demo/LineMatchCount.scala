package demo

import org.apache.spark.{SparkConf, SparkContext}

object LineMatchCount {

    def main(args: Array[String]) {
        val filename = "E:\\technology_workspace\\data\\commonwords.txt"
        val conf = new SparkConf().setMaster("local").setAppName("My App")
        val sc = new SparkContext(conf)
        val data = sc.textFile(filename)
        val numAs = data.filter(line => line.contains("a")).count()
        val numBs = data.filter(line => line.contains("b")).count()
        println(s"Lines with a: $numAs, Lines with b: $numBs")
        sc.stop()
    }
}
