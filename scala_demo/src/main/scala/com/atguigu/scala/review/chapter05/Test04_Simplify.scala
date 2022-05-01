package com.atguigu.scala.review.chapter05

/**
 *  @author Adam-Ma 
 *  @date 2022/5/1 13:31
 *  @Project BigData_Review_Adam
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*    函数至简原则
 *      （1）return 可以省略，Scala 会使用函数体的最后一行代码作为返回值
 *      （2）如果函数体只有一行代码，可以省略花括号
 *      （3）返回值类型如果能够推断出来，那么可以省略（:和返回值类型一起省略）
 *      （4）如果有 return，则不能省略返回值类型，必须指定
 *      （5）如果函数明确声明 unit，那么即使函数体中使用 return 关键字也不起作用
 *      （6）Scala 如果期望是无返回值类型，可以省略等号
 *      （7）如果函数无参，但是声明了参数列表，那么调用时，小括号，可加可不加
 *      （8）如果函数没有参数列表，那么小括号可以省略，调用时小括号必须省略
 *      （9）如果不关心名称，只关心逻辑处理，那么函数名（def）可以省略
 */
object Test04_Simplify {
  def main(args: Array[String]): Unit = {
    def fun1(name : String): String ={
      println("你的名字为：" + name)
      return name
    }

    // 1、 return 可以省略
    def fun2(name : String): String ={
      println("你的名字为：" + name)
      name
    }

    // 2、如果函数体中只有一行代码，可以省略花括号
    def fun3(name : String): String = name

    // 3、如果返回值类型可以被推断出来，返回值类型也可以省略
    def fun4(name : String) = name

    // 4、如果有 return ，则不能省略返回值类型
//    def fun5(name : String) ={
//      return name
//    }

    // 5、返回值类型为 Unit ，有return 也不管用
    def fun5(name : String): Unit ={
      return name
    }

    println(fun5("张三"))   // 返回 Unit 的对象：()

    // 6、如果期望是无返回值类型，则可以省略 =
    def fun6(name : String){
      println(name)
    }
    fun6("zhan")

//    （7）如果函数无参，但是声明了参数列表，那么调用时，小括号，可加可不加
    def fun7(): Unit = {
      println("Hello")
    }
    // 调用
    // 加 小括号调用
    fun7()
    // 不加 小括号调用
    fun7

//    （8）如果函数没有参数列表，那么小括号可以省略，调用时小括号必须省略
    def  fun8: Unit = {
      println("无参数列表")
    }
    // 调用 无参数列表的函数,必须省略小括号
    fun8
    // 加了小括号会报错
//    fun8()

//    （9）如果不关心名称，只关心逻辑处理，那么函数名（def）可以省略
  val a =  (name:String) => println(name)    // 匿名函数 : lambda 表达式
    a("Adma")
  }
}
