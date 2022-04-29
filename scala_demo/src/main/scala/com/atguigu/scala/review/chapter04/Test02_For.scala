package com.atguigu.scala.review.chapter04
import scala.collection.immutable

/**
 *  @author Adam-Ma 
 *  @date 2022/4/29 20:57
 *  @Project BigData_Review_Adam
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*   Scala 中的 for ：
 *    1、对范围数据的遍历
 *       1-1 包含边界    ： for(i <- 1 to    10)
 *       1-2 不包含边界  ： for(i <- 1 until 10)
 *    2、对集合类型数据的遍历
 *       for(i <- Array(23,4,5l,435))
 *       for(i <- List(34,23,43,54))
 *    3、循环守卫：
 *        Scala 中无 continue 关键字，使用循环守卫替代
 *        for (i <- 1 to 10 if i != 5){
 *          println(i)
 *        }
 *    4、循环步长：by
 *       底层： for(i <- new Range.Inclusive(1,10,2)){}
 *       简便： for(i <- 1 to 10 by 2)
 *             for(i <- 10 to 1 by -2)
 *             for(i <- 1.0 to 10.0 by 0.5)
 *             for(i <- 1 to 10 by 0)  ----> 报错，步长不可以为0
 *    5、嵌套循环：
 *        5-1 for(i <- 1 to 3){
 *              for(j <- 1 to 2){}
 *            }
 *        5-2 for(i <- 1 to 3; j <- 1 to 2){}
 *        5-3 for{i <- 1 to 3
 *                j <- 1 to 2
 *                k <- 1 to 2
 *              }{具体的代码}
 *    6、引入变量 ：
 *        6-1 for（i <- 1 to 9; j = i + 3) {}
 *        6-2 for {
 *                i <- 1 to 9
 *                j = i + 3
 *           }{}
 *    7、循环返回值： yield
 *       val result: immutable.IndexedSeq[Int] = for(i <- 1 to 10) yield i * i
 *    8、循环倒序：reverse
 *        for(i <- 1 to 10 reverse)
 *
 */
object Test02_For {
  def main(args: Array[String]): Unit = {
    // 1、对范围数据的遍历
      // 1-1 包含边界
    for (i <- 1 to 10) {
      println(i)
    }

    println("=" * 50)
      // 1-2 不包含边界
    for (i <- 1 until 10) {
      println(i)
    }

    println("=" * 50)
    // 2、对集合数据的遍历
    for (i <- Array(324,23,453,54)){
      println(i)
    }

    println("=" * 50)
    for (i <- List(234,45,56,76)){
      println(i)
    }

    println("=" * 50)
    // 3、循环守卫
    for (i <- 1 to 10 if i != 5) {
      println(i)
    }

    // 4、循环步长
    println("=" * 50)
    // 底层
    for (i <- new Range.Inclusive(1,10,2)){
      println(i)
    }

    // 简便：by
    println("=" * 50)
    for (i <- 1 to 10 by 2){
      println(i)
    }

    println("=" * 50)
    for (i <- 1.0 to 10.0 by 1.5) {
      println(i)
    }

    // 小数存在不精确的问题，如下
    println("=" * 50)
    for (i <- 1.0 to 3.0 by 0.3) {
      println(i)
    }

    // 步长可以为负数
    println("=" * 50)
    for(i <- 10 to 1 by -2) {
      println(i)
    }

    // 5、循环嵌套：
    /*
        5-1 : for(i <- 1 to 3){
                for(j <- 1 to 2){}
              }
     */
    println("=" * 50)
    for (i <- 1 to 3) {
      for(j <- 1 to 2) {
        println(s"i = ${i} , j = ${j}")
      }
    }

    /*
       5-2 : for(i<-1 to 3; j<- 1 to 2){}
     */
    println("=" * 50)
    for (i <- 1 to 3; j <- 1 to 2) {
      println(s"i = ${i}, j = ${j}")
    }

    /*
      5-3 : for { i <- 1 to 3
                  j <- 1 to 2
                  k <- 1 to 2
                }{}
     */
    println("=" * 50)
    for {
      i <- 1 to 3
      j <- 1 to 2
      k <- 1 to 2
    }{
        println(s"i = ${i}, j = ${j}, k = ${k}")
    }

    // 6、引入变量
    println("=" * 50)
    for(i <- 1 to 9; j = i + 3){
      println(s"i = $i, j = $j")
    }

    println("=" * 50)
    for {
      i <- 1 to 9
      j = i + 3
    }{
      println(s"i = $i, j = $j")
    }

    // 7、循环返回值 yield
    println("=" * 50)
    val result: immutable.IndexedSeq[Int] = for(i <- 1 to 5) yield i * i
    println(result)

    // 8、循环倒序
    println("=" * 50)
    for(i <- 1 to 5 reverse){
      println(i)
    }
  }
}
