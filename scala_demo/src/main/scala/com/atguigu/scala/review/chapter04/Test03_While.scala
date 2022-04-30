package com.atguigu.scala.review.chapter04

/**
 *  @author Adam-Ma 
 *  @date 2022/4/30 18:00
 *  @Project BigData_Review_Adam
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*     测试 Scala 中的 while 和 do…… while
 */
object  Test03_While {
  def main(args: Array[String]): Unit = {
    // while
    var num1 = 10
    while (num1 > 0) {
      println(num1)
      num1 -= 1
    }

    var num2 = 1
    // do …… while
    do {
      println(num2)
      num2 += 1
    }while(num2 < 10)

  }
}
