package com.atguigu.scala.review.chapter04

/**
 *  @author Adam-Ma 
 *  @date 2022/4/30 20:28
 *  @Project BigData_Review_Adam
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*    使用 Scala 打印 九层 妖塔
 */
object Problem02_Pyramid {
  def main(args: Array[String]): Unit = {
    // 方式一： Java 思维
    for (i <- 1 to 9) {
      // 打印空格
      for(j <- 1 to 9-i){
        print(" ")
      }
      // 打印 *
      for(k <- 1 until 2 * i){
        print("*")
      }
      // 换行
      println()
    }

    println("=" * 50)
    // Scala 思维：字符串可乘
    for (i <- 1 to 9) {
      // 打印空格
        print(" " * (9 - i))
      // 打印 *
      print("*" * (2 * i - 1 ))
      // 换行
      println()
    }

    println("=" * 50)
    // 方法二：
    for (i <- 1 to 9) {
      println(" " * (9 - i) + "*" * (2 * i - 1 ))
    }

    println("=" * 50)
    // Scala：引入变量
    for (i <- 1 to 9; spaces = 9 - i; stars = 2 * i - 1) {
      println(" " * spaces + "*" * stars)
    }
  }
}
