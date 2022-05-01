package com.atguigu.scala.review.chapter05

/**
 *  @author Adam-Ma 
 *  @date 2022/5/1 20:17
 *  @Project BigData_Review_Adam
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*   函数柯里化
 *     概念： 将函数的一个参数列表中的多个参数变为多个参数列表，称之为函数柯里化
 *     用到函数柯里化的地方一定用到了闭包
 *
 *  闭包：
 *      一个函数使用到外部（局部）的变量，该函数将外部的变量当作属性值 与当前函数打包在一起，形成一个闭合的环境，称之为闭包
 */
object Test09_Currying {
  def main(args: Array[String]): Unit = {
    // 使用闭包的方式完成练习 2
    def fun2(i: Int): String => Char => Boolean = {
      def fun_m(s: String): Char => Boolean = {
        def fun_i(c: Char): Boolean = {
          if (i == 0 && s == "" && c == '0') false else true
        }

        fun_i
      }

      fun_m
    }

    // 使用 函数柯里化的方式，完成如上功能
    def fun(i: Int)(s: String)(c: Char): Boolean = {
      if (i == 0 && s == "" && c == '0') false else true
    }

    // 调用函数
    println(fun(1)("")('0'))
  }
}
