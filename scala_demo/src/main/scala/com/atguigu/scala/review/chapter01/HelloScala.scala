package com.atguigu.scala.review.chapter01

/**
 *  @author Adam-Ma 
 *  @date 2022/4/28 14:34
 *  @Project BigData_Review_Adam
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
// object 定义一个scala 单例对象
object HelloScala {
  // 通过 def 定义一个函数
  // def 函数名(参数名 ：参数类型) ：返回值类型 = { 函数体 }
  // Unit 代表空值
  def main(args: Array[String]): Unit = {
    println("Hello Scala")
    // 也可以直接调用 Java 的类库
    System.out.println("Hello Java")
  }
}
