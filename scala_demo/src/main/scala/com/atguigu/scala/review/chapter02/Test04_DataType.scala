package com.atguigu.scala.review.chapter02

/**
 *  @author Adam-Ma 
 *  @date 2022/4/28 23:30
 *  @Project BigData_Review_Adam
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*   数据类型：
 *    Any :
 *      |-- AnyVal:
 *          |-- Byte
 *          |-- Short    |-- Char
 *          |-- Int
 *          |-- Long
 *          |-- Float
 *          |-- Double
 *          |-- Boolean
 *          |-- Unit （空值类型，打印() ）
 *          |-- StringOps （数据值类型的 String ，是对 java 中 String 的优化 ）
 *
 *      |-- AnyRef:
 *          |-- Scala 中的集合
 *          |-- Scala 中的类
 *          |-- Java 中的类 (例如 String)
 *          |-- Null
 *
 *      Nothing : 所有类型的子类
 *
 *   1、Scala中依然有 低精度类型--》高精度类型，自动装换（隐式转换）
 *   2、Unit ： 对应Java中的void， 用于方法返回值的位置，表示方法没有返回值。Unit 是一个数据类型， 只有一个对象 () 。void 不是数据类型，只是一个关键字
 *   3、Null 是一个类型， 只有一个 对象就是 null 。 它是所有引用类型（AnyRef）的子类。
 *   4、当抛出异常的时候，如果抛出了 Nothing ，为了精确，我们可以将抛出的异常精确到具体类型，因为 Nothing是所有类型的子类型
 */
object Test04_DataType {
  def main(args: Array[String]): Unit = {
    // 1、定义 byte
    val b1 = 97
//    val b2:Byte = 97 +  23  // 虽然IDEA会报错，但是对于整数据来说，由于在 Byte 的数据范围内，所以运行的时候是没问题哒
//    println(b2.getClass)

    // 2、 byte的变量相加
    val b3:Int = b1 + 12
    val b4:Byte = (b3 + 23).toByte

    // 3、Byte 的范围
    val b5 = 128
    println(b5.getClass)

    // 4、定义 Char 型的数据
    val c1 : Char = 'a'
    val c2 = c1 + 2   // Int 类型
    println(c2)   // 99

//    val c3 : Char = 'a' + 2// 虽然报错，但运行没有问题的， 数值型的常量直接运算，不会进行类型转换的
//    println(c3)

    // 4、定义 Long 类型的变量
    val L1 : Long = 14353443243142L
    println(L1.getClass)

    // 5、Float 类型的变量的定义
    val f1 : Float = 234.345F
    println(f1.getClass)

    // 6、 Double 类型的变量的定义
    val d1 : Double = 234.55
    println(d1.getClass)

    // 7、空类型 ： Unit
    def fun(): Unit = {
      println("我是 Unit 类型，我只有一个对象：() , 我的父类是 AnyVal")
    }

    val fun1 : Unit = fun()
    println(fun1 + "123")   // 会打印 ： () 123

    // 8、定义 抛出异常的

    // 一般抛出异常，会返回 Nothing
    def fun2(num1 : Int) : Int = {
      if (num1 == 0) {
        throw new NullPointerException
      }else
        return num1 + 1
    }
  }
}



























































