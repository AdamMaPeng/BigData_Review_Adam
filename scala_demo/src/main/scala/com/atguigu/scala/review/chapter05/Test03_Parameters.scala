package com.atguigu.scala.review.chapter05

/**
 *  @author Adam-Ma 
 *  @date 2022/5/1 10:57
 *  @Project BigData_Review_Adam
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*   函数参数：
 *    （1）可变参数
 *    （2）如果参数列表中存在多个参数，那么可变参数一般放置在最后
 *    （3）参数默认值，一般将有默认值的参数放置在参数列表的后面
 *    （4）带名参数
 *
 */
object Test03_Parameters {
  def main(args: Array[String]): Unit = {
//    （1）可变参数
    def fun1(name : String *): Unit ={
      println(name)
    }
//    （2）如果参数列表中存在多个参数，那么可变参数一般放置在最后
    def fun2( age : Int , name : String * ): Unit ={

    }
//    （3）参数默认值，一般将有默认值的参数放置在参数列表的后面
    def fun3(org : String = "尚硅谷", name : String): Unit ={
      println(s"${name} 在 ${org} 学习")
    }

//    （4）带名参数
    fun3(name = "Adam", org =" 传智播客")

    // 调用 fun1
    println(fun1("Tom", "Amy"))
  }
}
