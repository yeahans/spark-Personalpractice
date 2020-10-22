package com.offcn.bigdata.day06

import org.apache.spark.{SparkConf, SparkContext}

object TransformationOops {
  def main(args: Array[String]): Unit = {
    val conf  = new SparkConf()
      .setAppName("TransformationOps")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    abk2rbkOps(sc)
  }

  def abk2rbkOps(sc:SparkContext): Unit ={
    val lines = sc.parallelize(List(
      "hello you",
      "hello me",
      "hello lan lan"
    ))

    val pairs = lines.flatMap(_.split("\\s+").map((_,1)))
    def createCombiner(num:Int): Int ={
      num
    }
    def mergeValue(sum:Int,num:Int):Int = {
      sum + num
    }
    def mergeCombiners(sum1:Int,sum2:Int):Int = {
      sum1+sum2
    }
    val ret = pairs.aggregateByKey(0)(mergeValue, mergeCombiners)
    ret.foreach(println)
  }
}
