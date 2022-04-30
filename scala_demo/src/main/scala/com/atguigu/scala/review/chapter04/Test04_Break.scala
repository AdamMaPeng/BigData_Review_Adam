package com.atguigu.scala.review.chapter04

import scala.util.control.Breaks

/**
 *  @author Adam-Ma 
 *  @date 2022/4/30 19:36
 *  @Project BigData_Review_Adam
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*   测试 Scala 中的 Break （跳出循环）
 *    在 Java 中，跳出循环可以采用，
 *      1、break 的方式跳出循环，
 *      2、抛出异常的方式跳出循环
 *
 *    Scala 中 没有 break关键字，所以可以使用
 *      1、抛出异常的方式
 *        val i = 1
 *        try{
 *           if (i == 1) {
 *              throw new RuntimeException
 *            }
 *        }catch {
 *            case e : Exception =>
 *        }
 *      2、Breaks.breakable{
 *          Breaks.break()
 *      }
 */
object Test04_Break {
  def main(args: Array[String]): Unit = {

    // 1、抛出异常的方式
    try{
      for (i <- 1 to 10){
        if (i == 5)
          throw new RuntimeException()
        println(i)
      }
    }catch {
      case e : Exception =>
    }
    println("循环外的代码")

    println("=" * 50)
//     2、Breaks.breakable的方式
    Breaks.breakable(
      for (i <- 1 to 10) {
        if (i == 5){
          Breaks.break()
        }
        println(i)
      }
    )
    println("循环外的代码")

    // 3、import Breaks的包，然后在代码中不用写 Break
  }
}
