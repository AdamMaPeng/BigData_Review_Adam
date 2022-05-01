package com.atguigu.scala.review.chapter05

/**
 *  @author Adam-Ma 
 *  @date 2022/5/1 16:26
 *  @Project BigData_Review_Adam
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*   高阶函数的应用
 */
object Test07_ApplicationCase {
  def main(args: Array[String]): Unit = {
    // 定义一个函数，对数组中所有元素进行处理，处理完成之后，返回一个新的数组，当前的处理操作是可变的，作为参数传入
    def arrayOperation(array:Array[Int] , op: Int => Int) : Array[Int] = {
      // op 是针对 每个元素进行的操作，需要遍历数组
      for (elem <- array) yield op(elem)
    }

    def op1 (num1 : Int) : Int ={
      num1 * 3
    }

    val array1 = Array(1,2,3,4,5)

    val newArray1: Array[Int] = arrayOperation(array1, op1)
    println(newArray1.mkString(","))

    println(arrayOperation(array1, (a: Int) => a * a).mkString(","))
    println(arrayOperation(array1, _ + 5).mkString(","))

  }
}
