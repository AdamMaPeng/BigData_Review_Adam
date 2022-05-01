package com.atguigu.scala.review.chapter05

/**
 *  @author Adam-Ma 
 *  @date 2022/5/1 21:11
 *  @Project BigData_Review_Adam
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*   自定义 While
 *    通过 控制抽象的方式完成自定义 while 循环
 */
object Test13_MyWhile {
  def main(args: Array[String]): Unit = {
    var n = 10
    while (n > 0){
      println(n)
      n -= 1
    }

    println("=" * 50)
    // 定义 myWhile 函数
    def myWhile(condition: Boolean)(op : => Unit): Unit ={
      if (condition) {
        op
        myWhile(condition)(op)
      }
    }

    myWhile(n > 0)({
      println(n)
      n -= 1
    })
  }
}
