package com.atguigu.scala.review.chapter05

/**
 *  @author Adam-Ma 
 *  @date 2022/5/1 20:24
 *  @Project BigData_Review_Adam
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*   函数的递归：
 *    完成 数的阶乘
 */
object Test10_Recurrsion {
  def main(args: Array[String]): Unit = {
    def recur(n : Int) : Int ={
      if (n == 0) return 1
      recur(n - 1) * n
    }

    println(recur(5))
  }
}
