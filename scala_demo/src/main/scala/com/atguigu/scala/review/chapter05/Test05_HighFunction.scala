package com.atguigu.scala.review.chapter05

/**
 *  @author Adam-Ma 
 *  @date 2022/5/1 14:06
 *  @Project BigData_Review_Adam
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*   高阶函数：
 *      1） 函数作为值进行传递
 *      2） 函数作为参数进行传递
 *      3） 函数作为返回值进行传递
 *
 *
 */
object Test05_HighFunction {
  def main(args: Array[String]): Unit = {
    // 定义lambda 表达式
//    val f1 = (name : String) => println(name)

    def fun(): Unit ={
      println("Hello")
    }
//    1） 函数作为值进行传递
    // (1) 调用 fun 函数，将返回值给变量 f1, f2
//    val f1 = fun()
//    val f2 = fun
//    println(f1)

    // (2) 将 fun 函数作为一个整体，传递给 变量 f
    val f = fun _
    // 函数调用
    fun()
    f()

    // (3) 如果明确变量类型，不使用下划线也可以进行整体传递
    val f3 : () => Unit = fun
    f3()

//    2） 函数作为参数进行传递
    def fun2(f: (Int ,Int) => Int ) {
      f(3,5)
    }

    def add(a:Int, b:Int):Int = a + b

    // 将add 作为参数传入 fun2 中
    fun2(add)
    fun2((a:Int,b:Int) => a + b)
    fun2(_ + _)

//    3） 函数作为返回值进行传递
    def fun4(): Unit ={
      def f5()={
        println("Hi")
      }
      f5 _
    }

   val ff  = fun4()

  }
}
