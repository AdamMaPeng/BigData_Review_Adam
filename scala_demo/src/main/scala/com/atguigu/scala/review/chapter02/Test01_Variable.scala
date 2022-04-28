package com.atguigu.scala.review.chapter02

import com.atguigu.scala.review.chapter01.Student

/**
 *  @author Adam-Ma 
 *  @date 2022/4/28 18:58
 *  @Project BigData_Review_Adam
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*   测试变量相关内容：
 *      1） 声明变量时，类型可以省略，编译器自动推导，既类型推导
 *      2） 类型确定后，就不能修改，说明Scala 是强数据类型语言
 *      3） 变量声明时，必须要有初始值
 *      4）在声明/定义一个变量时，可以使用 var 或者val来修饰， var 修饰的变量可改变，val 修饰的变量不可改
 */
class Test01_Variable {

}

object Test01_Variable{
  def main(args: Array[String]): Unit = {
    // 1) 声明变量时，类型可以省略，编译器自动推导，既类型推导
    val a1 : String = "a1 没有省略数据类型"
    var a2 = "a2 省略了数据类型"

    // 2) 类型确定后，就不能修改，说明 Scala 是强数据类型的语言
    var n1 = 123
    // 声明为 var的变量是可以更改的，但是类型不可改
    //    n1 = 123.0

    // 3) 变量声明时，必须要有初始值
    //    var n2 : String;   // 显然是不行的，会直接报错

    // 4） 在声明/定义一个变量时，可以使用 var 或者 val 来修饰，var 修饰的变量可改变，val 修饰的变量不可改
    /*
      对于 伴生类来说：
         class Student(name: String, age: Int){}          :  name，age 相当于 构造函数中的参数， 由 private final 修饰，不能访问更无法修改
         class Student(val name: String, val age: Int){}  :  name, age 可以被访问，但是不可以被修改
         class Student(var name: String, var age: Int){}  :  name, age 可以被访问，可以被修改
     */
    val s1 = new Student("Adam", 25)
    val s2 = new Student("Leo", 20)

    println(s1.age)    // 定义为 val 时，可以访问，但是不可以修改
    println(s1.name)

    s1.age = 3   // 定义为 var 时可以访问，可以修改
  }
}

