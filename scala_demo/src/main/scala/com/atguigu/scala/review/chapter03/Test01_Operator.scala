package com.atguigu.scala.review.chapter03

/**
 *  @author Adam-Ma 
 *  @date 2022/4/29 19:07
 *  @Project BigData_Review_Adam
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*   Scala中的 运算符
 *     特殊：
 *        1、Scala中的 == ，对于对象来说，效果就是 equals ，比较的就是两个对象的内容是否相等
 *          如果比较地址，使用 a.eq(B)
 *        2、Scala中没有 ++ ，-- ; 使用 +=， -= 来代替
 */
object Test01_Operator {
  def main(args: Array[String]): Unit = {
    // 算术运算符
    val a = 21 % -4
    println(a)

    // 比较运算符
    val s1 : String = "hello"
    val s2 : String = new String("hello")
    // == : 等同于 equals比较两个对象的内容是否相同
    println(s1 == s2)
    // equals :比较两个对象的内容是否相同
    println(s1.equals(s2))
    // eq() ： 比较两个对象的地址是否相同
    println(s1.eq(s2))

    // 逻辑运算符
    def isEmptyStr(str : String): Boolean ={
      return str == null && "".equals(str.trim)
    }

    println("判断 s1 是否为空串 : " + isEmptyStr(s1))
  }
}
