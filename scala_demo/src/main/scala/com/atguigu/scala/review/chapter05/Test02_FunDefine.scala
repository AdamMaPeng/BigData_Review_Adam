package com.atguigu.scala.review.chapter05

/**
  *  @author Adam-Ma
  *  @date 2022/5/1 10:39
  *  @Project BigData_Review_Adam
  *  @email Adam_Ma520@outlook.com
  *  @phone 18852895353
  */
/**
  *    定义 函数
  *      （1）函数 1：无参，无返回值
  *      （2）函数 2：无参，有返回值
  *      （3）函数 3：有参，无返回值
  *      （4）函数 4：有参，有返回值
  *      （5）函数 5：多参，无返回值
  *      （6）函数 6：多参，有返回值
  */
object Test02_FunDefine {
  def main(args: Array[String]): Unit = {
//    （1）函数 1：无参，无返回值
    def fun1(): Unit = {
      println("1、定义无参、无返回值的函数")
    }

//    （2）函数 2：无参，有返回值
    def fun2(): Int = {
      println("2、定义无参、有返回值的函数")
      25
    }

//    （3）函数 3：有参，无返回值
    def fun3(name : String): Unit ={
      println("3、定义有参、无返回值的函数")
    }

//    （4）函数 4：有参，有返回值
    def fun4(name : String): String ={
      println("4、定义有参、有返回值的函数")
      name
    }

//    （5）函数 5：多参，无返回值
    def fun5(name : String, age : Int): Unit ={
      println(s"${name} 今年 ${age} 岁了")
    }

//    （6）函数 6：多参，有返回值
    def fun6(name : String, age : Int): Boolean ={
      println(s"${name} 今年 ${age} 岁了")
      true
    }

    // 1) 调用 fun1
    fun1()
    fun2()
    fun3("Leo")
    println(fun4("Adam"))
    fun5("Jack", 25)
    println(fun6("Amy", 18))
  }
}
