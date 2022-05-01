package com.atguigu.scala.review.chapter05

/**
 *  @author Adam-Ma 
 *  @date 2022/5/1 20:39
 *  @Project BigData_Review_Adam
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*   控制抽象：
 *     值调用： 在函数的参数列表中传入值
 *     名调用： 在函数的参数列表中传入代码块
 *
 *     代码块： 参数列表不存在的代码
 *         def f2(b: => Int): Unit = {}
 *         其中 b: => Int 就是代码块
 *
 */
object Test11_ControlAbstract {
  def main(args: Array[String]): Unit = {
    // 定义值调用函数
    def f0(i : Int): Int = {
      println("您打印的数为" + i)
      i
    }

    def f1() : Int = {
      15
    }

    //  值调用
    f0(12)
    f0(f1())

    // 定义名调用函数
    def f2(b: => Int): Unit = {
      println("您打印的数为" + b)
    }

    // 定义需要传入名调用函数的函数
    f2(f0(23))

    f2({
      println("222")
      222
    })

  }
}
