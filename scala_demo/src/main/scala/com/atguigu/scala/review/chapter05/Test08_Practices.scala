package com.atguigu.scala.review.chapter05

/**
 *  @author Adam-Ma 
 *  @date 2022/5/1 16:37
 *  @Project BigData_Review_Adam
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*   练习 1.多参数函数的匿名实现
            用匿名函数实现这样一个功能：传入三个参数，类型分别为Int、String、Char，返回值为Boolean
            判断当前的所有参数不为0值

        2.高阶函数的实现
            函数有三层嵌套，每层函数只有一个参数，外层函数返回中层函数，中层函数返回内层函数，最内层函数返回Boolean
            功能跟练习1类似，做不为0值的判断
            fun(0)("")('0')返回false，否则都返回true
 */
object Test08_Practices {
  def main(args: Array[String]): Unit = {
    // 练习1 ： 多参数的匿名实现
    def fun1(a:Int, b:String, c:Char) : Boolean ={
      if (a==0 && b.toInt == 0 && c.toInt == 0)
        false
      true
    }

    val fun = (a:Int, b:String ,c:Char) => if (a==0 && b == "" && c == '0') false else true
    println(fun(1, "0", '0'))
    println(fun(0, "", '0'))

    // 练习 2 ： 高阶函数的实现
    def fun2(i : Int) : String => Char => Boolean = {
      def fun_m(s : String) : Char => Boolean ={
        def fun_i(c : Char): Boolean ={
          if (i==0 && s == "" && c == '0') false else true
        }
        fun_i
      }
      fun_m
    }

    println(fun2(1)("")('0'))

    // 使用函数柯里化的方式完成练习2
    def fun3(i : Int)(s : String)(c : Char): Boolean ={
      if (i==0 && s == "" && c == '0')  false else true
    }

    println(fun3(0)("")('0'))
    println(fun3(0)("1")('0'))
    println(fun3(0)("")('1'))
  }
}
