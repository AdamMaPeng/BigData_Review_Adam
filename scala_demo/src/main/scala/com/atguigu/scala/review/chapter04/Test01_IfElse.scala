package com.atguigu.scala.review.chapter04

import scala.io.StdIn

/**
 *  @author Adam-Ma 
 *  @date 2022/4/29 20:08
 *  @Project BigData_Review_Adam
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*    Scala 中的 if-else
 *     1） 单分支
 *     2） 双分支
 *     3） 多分支
 *     4） 分支语句的返回值
 *     5） if-else 的嵌套
 *     6) if - else 代替三元运算符
 *          val str: String = if (num > 0 ) "正数" else "负数"
 */
object Test01_IfElse {
  def main(args: Array[String]): Unit = {
    // 提示用户输入
    println("请输入您的年龄： ")
    val age : Int = StdIn.readInt()

    println("==================单分支==================")
    //    1） 单分支
    if (age > 18) {
      println("成年")
    }

    println("==================双分支==================")
//    2） 双分支
    if (age < 18) {
      println("未成年")
    }else{
      println("成年")
    }

    println("==================多分支==================")
//    3） 多分支
    if (age < 18){
      println("未成年")
    }else if (age < 35) {
      println("青年")
    }else if (age < 60) {
      println("中年")
    }else {
      println("老年")
    }

    println("==================分支语句的返回值==================")
//    4） 分支语句的返回值
    val result: String = if (age < 18) {
      "未成年"
    } else if (age < 35) {
      "青年"
    } else if (age < 60) {
      "中年"
    } else {
      "老年"
    }
    println(result)

    println("==================分支语句的返回值==================")
//    5） if-else 的嵌套
    if (age > 0 && age < 18){
      println("未成年")
      if (age < 6) {
        println("婴幼儿")
      }else if (age < 12) {
        println("儿童")
      }else{
        println("青少年")
      }
    }else if (age < 35) {
      println("青年")
    }else if (age < 60) {
      println("中年")
    }else {
      println("老年")
    }

    // if - else 代替三元运算符
    val num = 10
    val str: String = if (num > 0 ) "正数" else "负数"
  }
}
