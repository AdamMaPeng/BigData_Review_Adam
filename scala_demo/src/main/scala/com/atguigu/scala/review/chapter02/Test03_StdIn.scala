package com.atguigu.scala.review.chapter02

import scala.io.StdIn

/**
 *  @author Adam-Ma 
 *  @date 2022/4/28 22:24
 *  @Project BigData_Review_Adam
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*  Scala 中的标准输入：
 *    import scala.io.StdIn
 */
object Test03_StdIn {
  def main(args: Array[String]): Unit = {
    println("请输入您的姓名")
    val name = StdIn.readLine()

    println("请输入您的年龄")
    val age = StdIn.readInt()

    println(s"欢迎${age}岁的${name}来尚硅谷学习")
  }
}
