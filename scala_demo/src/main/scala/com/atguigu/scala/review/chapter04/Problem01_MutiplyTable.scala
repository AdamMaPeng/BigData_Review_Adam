package com.atguigu.scala.review.chapter04

/**
 *  @author Adam-Ma 
 *  @date 2022/4/29 23:25
 *  @Project BigData_Review_Adam
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*   使用 Scala 完成 9 * 9 乘法表
 *
 */
object Problem01_MutiplyTable {
  def main(args: Array[String]): Unit = {
    // 传统 for 循环
    for (i <- 1 to 9) {
      for(j <- 1 to 9) {
        if (j <= i){
          print(s"${i} * ${j} = ${i * j}  ")
        }
      }
      println()
    }

    println("=" * 50)
    // Scala 中 的 for 循环
    for(i <- 1 to 9 ; j <- 1 to 9 ) {
      print(s"${i} * ${j} = ${i * j}  ")
      if (i == j) println()
    }
  }
}
