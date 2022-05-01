package com.atguigu.scala.review.chapter05

/**
 *  @author Adam-Ma 
 *  @date 2022/5/1 9:50
 *  @Project BigData_Review_Adam
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*    Scala 中的 函数 & 方法
 *      函数：未完成某一个功能的程序语句的集合
 *      方法： 在类或者对象中定义的函数
 *
 *      方法可以重写和重载 ； 函数不能重写和重载
 *      函数可以嵌套
 */
object Test01_Function {
  def main(args: Array[String]): Unit = {
    // 定义函数
    def sayHi(name : String): Unit ={
      println("hi, " + name)
    }

    // 以下代码错误，函数是不能重载和重写的
//    def sayHi(name:String, age : Int): Unit ={
//      println(s"Hi,${age}岁的" + name)
//    }

    // 定义嵌套函数
    def innerFun(name: String): Unit ={
        def say(): Unit ={
          println("我叫 " + name)
        }
      say()
      println("你好呀！")
    }

    // 调用函数
    sayHi("Adam")

    // 调用嵌套函数
    innerFun("Leo")

    // 调用方法
    Test01_Function.sayHi("Jeo")
    // 调用重载的方法
    Test01_Function.sayHi("Meo", 23)

  }

  // 定义方法
  def sayHi(name : String): Unit ={
    println("Hi" + name)
  }

  // 方法的重载
  def sayHi(name:String, age : Int): Unit ={
    println(s"Hi,${age}岁的" + name)
  }
}
