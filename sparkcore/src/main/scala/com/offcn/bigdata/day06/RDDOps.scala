package com.offcn.bigdata.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDOps {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(s"${RDDOps.getClass.getSimpleName}")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
   /* val list = 1 to 7
    val listRDD:RDD[Int] = sc.parallelize(list)
    listRDD.map(_*7).foreach(println)*/
    // flatMapOps(sc)
    // filterMapOps(sc)
    // sampleMapOps(sc)
    // unionMapOps(sc)
    // joinedMapOps(sc)
    // groupByKeyOps(sc)
    cbk2GbkOps(sc)


  }

  // 模拟groupByKey
  def cbk2GbkOps(sc:SparkContext): Unit ={
    val stuList = List(
      "白普州,1904-bd-bj",
      "伍齐城,1904-bd-bj",
      "曹佳,1904-bd-sz",
      "刘文浪,1904-bd-wh",
      "姚远,1904-bd-bj",
      "匿名大哥,1904-bd-sz",
      "欧阳龙生,1904-bd-sz"
    )
    val stuRDD = sc.parallelize(stuList)

    val class2InfoRDD:RDD[(String,String)] = stuRDD.map{
      line =>{}
        val dotIndex = line.lastIndexOf(",")
        val className = line.substring(dotIndex+1)
        (className,line)
    }

    println("=============groupByKey==============")
    val gbkRDD:RDD[(String,Iterable[String])] = class2InfoRDD.groupByKey()
    gbkRDD.foreach(println)


  }
  def groupByKeyOps(sc:SparkContext): Unit ={
    //stu表：id, name, gender, age, class
    val stuList = List(
      "1,白普州,1,22,1904-bd-bj",
      "2,伍齐城,1,19,1904-bd-wh",
      "3,曹佳,0,27,1904-bd-sz",
      "4,姚远,1,27,1904-bd-bj",
      "5,匿名大哥,2,17,1904-bd-wh",
      "6,欧阳龙生,0,28,1904-bd-sz"
    )
    val stuRDD = sc.parallelize(stuList)

    val class2InfoRDD:RDD[(String,String)] = stuRDD.map(
      line=>{
        val dotIndex = line.lastIndexOf(",")
        val className = line.substring(dotIndex+1)
        val info = line.substring(0,dotIndex)
        (className,info)
      }
    )

    val gbkRDD:RDD[(String,Iterable[String])] = class2InfoRDD.groupByKey()
    gbkRDD.foreach(println)

  }

  def joinedMapOps(sc:SparkContext): Unit ={
    val stuList = List(
      "1 严文青 女 18",
      "2 王大伟 男 55",
      "3 贾静凯 男 33",
      "4 old李 ladyBoy 31"
    )

    val scoreList = List(
      "1 语文 59",
      "3 数学 0",
      "2 英语 60",
      "5 体育 99"
    )

    val stuListRDD = sc.parallelize(stuList)
    val sid2StuInfoRDD:RDD[(Int,String)] = stuListRDD.map(
      line =>{
        val sid = line.substring(0,line.indexOf(" ")).toInt
        val info = line.substring(line.indexOf(" ")+1)
        (sid,info)
      }
    )

    val socerListRDD = sc.parallelize(scoreList)
    val sid2ScoreInfoRDD:RDD[(Int,String)] = socerListRDD.map(
      line =>{
        val sid = line.substring(0,line.indexOf(" ")).toInt
        val scoreInfo = line.substring(line.indexOf(" ")+1)
        (sid,scoreInfo)
      }
    )
    // 查询有成绩的学生信息
    val stuScoreInfoRDD:RDD[(Int,(String,String))] = sid2StuInfoRDD.join(sid2ScoreInfoRDD)
    stuScoreInfoRDD.foreach {
      case(sid,(stuInfo,scoreInfo))=>
        println(s"sid:$sid,stu's info: $stuInfo, stu's score: $scoreInfo")

    }

    // 查询所有学生的信息
    val stuInfo:RDD[(Int,(String,Option[String]))] = sid2StuInfoRDD.leftOuterJoin(sid2ScoreInfoRDD)
    stuInfo.foreach{
      case(sid,(stuInfo,scoreOption))=>
        println(s"sid:${sid},stu's info:${stuInfo},stu's score:${scoreOption.orNull}")

    }

    // 查询学生，及其有成绩的学生信息
    val stuScoreInfo:RDD[(Int,(Option[String],Option[String]))] = sid2StuInfoRDD.fullOuterJoin(sid2ScoreInfoRDD)
    stuScoreInfo.foreach{
      case(sid,(stuOption,scoreOption)) =>
        println(s"sid:${sid},stu's info:${stuOption.orNull},stu's score:${scoreOption.orNull}")

    }


  }

  /**
   * 使用unoin会将两个集合的元素放在一起，不会去重
   */
  def unionMapOps(sc:SparkContext): Unit ={
    val listRDD1 = sc.parallelize(1.to(10))
    val listRDD2 = sc.parallelize(5.to(15))
    listRDD1.union(listRDD2).foreach(println)
  }

  /**
   * flatMap示例
   * rdd.flatMap(func):RDD =>rdd集合中的每一个元素，都要作用function函数
   * 返回0到多个新的元素，these element make up a new RDD
   * map is one-2-one
   * flatMap is one to many operation
   */
  def flatMapOps(sc:SparkContext): Unit ={
    val list = List(
      "jia jing kan kan kan ",
      "gao di di di di",
      "zhang yuan qi qi",
      "wuyanjun an an"
    )
    val listRDD = sc.parallelize(list)
    listRDD.flatMap(_.split("\\s+")).foreach(println)
  }

  /**
   * 过滤掉返回值为false的值
   */
  def filterMapOps(sc:SparkContext): Unit ={
    val list = 1.to(100)
    sc.parallelize(list).filter(_%2==0).foreach(println)
  }

  /**
   * 抽样，需要注意而是spark中的sample抽样并不是十分精确的抽样
   * 它的作用主要是看rdd中数据的分布情况，从而让我们根据数据的情况进行调优，用以解决
   * 数据倾斜等问题
   * @param sc
   *
   * 需要知道的参数
   * withReplacement 抽样的方式，true有放回抽样，false为无放回抽样
   * fraction：抽样比例，取值范围是0到1之间
   * seed：抽样的随机数种子，有默认值，通常也不需要自己传值
   */
  def sampleMapOps(sc:SparkContext): Unit ={
    val listRDD = sc.parallelize(1 to 10000)
    var sampleRDD  = listRDD.sample(true,.001)
    println("样本空间的个数"+sampleRDD.count())
    sampleRDD = listRDD.sample(false,.001)
    println("不返回抽样的个数"+sampleRDD.count())
  }
}
