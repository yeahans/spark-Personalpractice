package com.offcn.bigdata.day07

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable
import scala.math.Ordering.ordered

object SparkCoreHomework {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("PersonInfo")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    // peopleInfoOps(sc)
    // userFollowInfo(sc)
    studentAnalyze(sc)

  }
  def studentAnalyze(sc:SparkContext): Unit ={
    val studentInfoRDD =  sc.parallelize(List(
      StudentInfo(12,"张三",25,"男","chinese",50),
      StudentInfo(12,"张三",25,"男","math",60),
      StudentInfo(12,"张三",25,"男","english",70),
      StudentInfo(12,"李四",20,"男","chinese",50),
      StudentInfo(12,"李四",20,"男","math",50),
      StudentInfo(12,"李四",20,"男","english",70),
      StudentInfo(12,"王芳",19,"女","chinese",70),
      StudentInfo(12,"王芳",19,"女","math",70),
      StudentInfo(12,"王芳",19,"女","english",70),
      StudentInfo(13,"张大三",25,"男","chinese",60),
      StudentInfo(13,"张大三",25,"男","math",60),
      StudentInfo(13,"张大三",25,"男","english",70),
      StudentInfo(13,"李大四",20,"男","chinese",50),
      StudentInfo(13,"李大四",20,"男","math",60),
      StudentInfo(13,"李大四",20,"男","english",50),
      StudentInfo(13,"王小芳",19,"女","chinese",70),
      StudentInfo(13,"王小芳",19,"女","math",80),
      StudentInfo(13,"王小芳",19,"女","english",70)
    ))
    // 参加考试的人数
    val num2test =  studentInfoRDD.map(_.name).distinct().count()
    println(s"参加考试人数为   ${num2test} 人")

    // 一共有多少个小于20岁的人参加考试？
    val ageAnalyze = studentInfoRDD.map(x=> (x.name,x.age)).distinct()
    val agelt20 =  ageAnalyze.filter(x=>x._2<20).count()
    println(s"年龄小于20的人有  $agelt20 人")

    // 一共有多少个等于20岁的人参加考试？
    val ageeq20 =  ageAnalyze.filter(x=>x._2==20).count()
    println(s"年龄等于20的人有  $ageeq20 人")

    // 一共有多少个大于20岁的人参加考试？
    val agert20 =  ageAnalyze.filter(x=>x._2>20).count()
    println(s"年龄等于20的人有  $agert20 人")

    val sexAnalyze = studentInfoRDD.map(x=>(x.name,x.sex)).distinct()
    val maleCount = sexAnalyze.filter(x=>x._2=="男").count()
    println(s"参加考试的男生人数为：${maleCount} 人")
    println(s"参加考试的女生人数为：${num2test-maleCount} 人")

    // 12班有多少人参加考试？
    val classAnalyze = studentInfoRDD.map(x=>(x.classId,x.name)).distinct()
    val class12Num = classAnalyze.filter(x=>x._1==12).count()
    println(s"12班的参加考试的总人数为 ${class12Num} 人")
    println(s"13班参加考试的总人数为${num2test-class12Num} 人")

    val chineseScoreAnalyze =  studentInfoRDD.filter(x=>x.subject=="chinese")
    val chineseSumScore = chineseScoreAnalyze.map(_.score).reduce(_+_)
    // 不考虑有缺考同学的情况，
    println(s"语文科目的平均成绩为：${chineseSumScore/num2test}")


    val mathScoreAnalyze =  studentInfoRDD.filter(x=>x.subject=="math")
    val mathSumScore = mathScoreAnalyze.map(_.score).reduce(_+_)
    println(s"数学科目的平均成绩为：${mathSumScore/num2test}")

    // 英语科目的平均成绩
    val englishScoreAnalyze =  studentInfoRDD.filter(x=>x.subject=="english")
    val englishSumScore = englishScoreAnalyze.map(_.score).reduce(_+_)
    println(s"英语科目的平均成绩为：${englishSumScore/num2test}")

    // 每个人的平均成绩
    val singleScore = studentInfoRDD.map(x=>(x.name,x.score))
    singleScore.reduceByKey(_+_).foreach(x=>println(s"${x._1}的平均成绩为${x._2/3}"))

    val class12AvgScore = studentInfoRDD.filter(_.classId==12).map(_.score).reduce(_+_)/class12Num
    println(s"12班的平均成绩为${class12AvgScore}")
    val class12MaleSumScore = studentInfoRDD.filter(x=>x.classId==12 && x.sex=="男").map(_.score).reduce(_+_)
    // 算出12班参加考试的男生人数
    val class12MaleNum = studentInfoRDD.filter(x=>x.classId==12 && x.sex=="男").map(_.name).distinct().count()
    println(s"12班男生的平均成绩为${class12MaleSumScore/class12MaleNum}")

    // 12班女生的平均总成绩
    val class12FeMaleSumScore = studentInfoRDD.filter(x=>x.classId==12 && x.sex=="女").map(_.score).reduce(_+_)
    println(s"12班女生的平均成绩为${class12FeMaleSumScore/(class12Num-class12MaleNum)}")

    // 全校语文最高分
    val chineseMax = studentInfoRDD.filter(_.subject=="chinese").map(_.score).max()
    println(s"全校语文成绩最高分为 $chineseMax")

    val class12ChineseMin = studentInfoRDD.filter(x=>x.subject=="chinese"&& x.classId==12).map(_.score).min()
    println(s"12班语文成绩最低分为 $class12ChineseMin")


    val class13MathMax = studentInfoRDD.filter(x=>x.subject=="math"&& x.classId==13).map(_.score).max()
    println(s"13班数学成绩最高分为 $class13MathMax")

    // 成绩大于150的女生
    val famaleScorert150 =  studentInfoRDD.filter(_.sex=="女").map(x=>(x.name,x.score)).reduceByKey(_+_).filter(_._2>150).count()
    println(s"女生成绩大于150的有 $famaleScorert150 人")
  }

  def userFollowInfo(sc:SparkContext): Unit ={
    val userInfoRDD = sc.parallelize(
      List(
      "11111111 12743457",
      "11111111 16386587",
      "11111111 19764388",
      "11111111 12364375",
      "11111111 13426275",
      "11111111 12356363",
      "11111111 13256236",
      "11111111 10000032",
      "11111111 10000001",
      "11111111 10000001",
      "11111121 10000032"
      )
    )

    val userNum = userInfoRDD.flatMap(_.split("\\s+")).distinct().count()
    println(s"一共有 ${userNum} 个用户")

    val user2fans =  userInfoRDD.distinct()
    println(s"一共有${user2fans.count()}对(ID,ID)")
    user2fans.foreach(println)
    // 使用去重之后的数据
    val fansNum = user2fans.map(x=>{
      val str = x.split("\\s+")(1)
      (str,1)
    })
    val res = fansNum.reduceByKey((x1,x2)=>x1+x2)
    res.foreach(x=>println(s"用户${x._1} 的粉丝数量为 ${x._2}"))
    val partitionsNum = res.partitionBy(new HashPartitioner(1))
    partitionsNum.saveAsTextFile("C:/Users/wuyan/Documents/BaiduNetdiskDownload/a")

  }

  def peopleInfoOps(sc:SparkContext): Unit ={
    val peopleInfoRDD = sc.parallelize(
      List(
        PeopleInfo(1,'F',179) ,
        PeopleInfo(2,'M',178),
        PeopleInfo(3,'M',174) ,
        PeopleInfo(4,'F',165),
        PeopleInfo(5,'M',192) ,
        PeopleInfo(6,'F',160),
        PeopleInfo(7,'F',170) ,
        PeopleInfo(8,'M',178),
        PeopleInfo(9,'M',163) ,
        PeopleInfo(10,'F',170) ,
        PeopleInfo(11,'M',178),
        PeopleInfo(12,'M',174)
      )
    )


    // 男性总数      女性总数
    val sexInfo = peopleInfoRDD.map(
      people => (people.sex,1)
    )
    // (M,3),(F,3)
    val totalInfo = sexInfo.reduceByKey(_+_)
    totalInfo.filter(_._1=='M').foreach(x=>println("男性总数为  "+x._2))
    totalInfo.filter(_._1=='F').foreach(x=>println("女性总数为  "+x._2))


    // 男性最高身高
    val maleRDD = peopleInfoRDD.filter(_.sex=='M')
    maleRDD.sortBy(_.height,false).take(1).foreach(x=>println("男性最高身高为:"+x.height))

    // 女性最高身高
    val femaleRDD = peopleInfoRDD.filter(_.sex=='F')
    femaleRDD.sortBy(_.height,false).take(1).foreach(x=>println("女性最高身高为:"+x.height))

    // 男性最低身高
    maleRDD.sortBy(_.height,true).take(1).foreach(x=>println("男性最低身高为:"+x.height))

    // 女性最低身高
    femaleRDD.sortBy(_.height,true).take(1).foreach(x=>println("女性最低身高为:"+x.height))
  }

}

case class PeopleInfo(id:Int,sex:Char,height:Int)
case class StudentInfo(classId:Int,name:String,age:Int,sex:String,subject:String,score:Int)