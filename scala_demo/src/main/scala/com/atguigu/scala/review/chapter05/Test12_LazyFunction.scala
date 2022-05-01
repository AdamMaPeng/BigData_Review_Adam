package com.atguigu.scala.review.chapter05

/**
 *  @author Adam-Ma 
 *  @date 2022/5/1 21:03
 *  @Project BigData_Review_Adam
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*   惰性函数：
 *      当函数返回值被声明为 lazy 时，函数的执行将被推迟，知道我们首次对此取值，该函数才会被执行
 *
 */
object Test12_LazyFunction {

  def add(a: Int, b: Int): Int = {
    println("3. add 被调用")
    a + b
  }

  def main(args: Array[String]): Unit = {
    lazy val a : Int = add(2,34)
    println("1. a 被声明")
    println(s"2. a = ${a}")
  }
}
