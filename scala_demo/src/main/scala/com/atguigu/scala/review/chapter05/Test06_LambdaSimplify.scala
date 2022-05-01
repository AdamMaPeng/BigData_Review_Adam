package com.atguigu.scala.review.chapter05

/**
 *  @author Adam-Ma 
 *  @date 2022/5/1 14:59
 *  @Project BigData_Review_Adam
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*   匿名函数的简化：
 *      （1）参数的类型可以省略，会根据形参进行自动的推导
 *      （2）类型省略之后，发现只有一个参数，则圆括号可以省略；其他情况：没有参数和参
 *           数超过 1 的永远不能省略圆括号。
 *      （3）匿名函数如果只有一行，则大括号也可以省略
 *      （4）如果参数只出现一次，则参数省略且后面参数可以用_代替
 */
object Test06_LambdaSimplify {
  def main(args: Array[String]): Unit = {
    // 匿名函数的定义
    val f = (name:String) => println(name)

    // 扩展高阶函数功能
    def d(g: String => Unit): Unit ={
      g("Hello")
    }

//    （1）参数的类型可以省略，会根据形参进行自动的推导
    d((name) => {println(name)})

//    （2）类型省略之后，发现只有一个参数，则圆括号可以省略；其他情况：没有参数和参数超过 1 的永远不能省略圆括号。
    d(name => {println(name)})

//    （3）匿名函数如果只有一行，则大括号也可以省略
    d(name => println(name))

//    （4）如果参数只出现一次，则参数省略且后面参数可以用_代替
    d(println(_))

    // (5) 如果可以直接推断出，当前传入的println是一个函数操作，那么后面的（_）也可以省略
    d(println)

    // 举例说明：定义一个“二元运算”函数，只操作1和2两个数，不确定做什么操作，需要作为参数传入
    def dualFunctionWithOneAndTwo(op: (Int, Int) => Int ): Int = {
      op(1, 2)
    }

    val add = (a:Int, b:Int) => a + b
    val minus = (a : Int, b : Int) => a - b

    dualFunctionWithOneAndTwo((a : Int, b : Int) => a + b)
    // 参数类型可以省略，会根据形参进行自动的推导
    dualFunctionWithOneAndTwo((a,b) => a + b)

    // 参数只出现一次， 则 参数省略后参数可以用 _ 代替
    dualFunctionWithOneAndTwo(_ + _)

  }
}
