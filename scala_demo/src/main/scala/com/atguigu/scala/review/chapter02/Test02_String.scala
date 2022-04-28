package com.atguigu.scala.review.chapter02

/**
 *  @author Adam-Ma 
 *  @date 2022/4/28 21:45
 *  @Project BigData_Review_Adam
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*   一、scala 中的标识符：
 *      1、可以是以操作符开头，且只包含操作符 ： 例如： +-*#
 *      2、使用反引号 ` `包裹任意字符串，包裹关键字也不成问题
 *
 *  二、不同于Java 中的关键字
 *      object  ：  定义对象
 *      trait   ：  类似于 interface
 *      with    :   在类的继承中使用
 *      sealed  ：   封装类 （外部不可以继承，所有子类只能在当前类中实现）
 *      implicit：   隐式转换相关
 *      match   ：   模式匹配
 *      yield   ：   for 循环中，将返回值这个结果进行临时保存
 *
 *  三、定义字符串输出
 *      1、使用 “ + ” 进行连接
 *      2、* ： 将字符串进行多次拼接
 *      3、通过 % 进行传值：
 *          printf("%d岁的%s在尚硅谷学习",age,name)
 *          // d:digital  s:String
 *      4、字符串模板：
 *          println(s"${age}岁的${name} 在尚硅谷学习")
 *      5、格式化模板
 *          val num = 2.34556
 *          println(f"num = ${num}%3.2f")
 *          // 整个数的长度不少于3  ， 小数位保留2 位 、
 *      6、 三引号： sql 常用
 *          """
 *            |
 *            |""".stripMargin
 *
 *
 */
object Test02_String {
  def main(args: Array[String]): Unit = {
//    1、使用 “ + ” 进行连接
    val age = 25
    val name = "Adam"
    println(name + "今年" + age + "在尚硅谷学习")

//    2、* ： 将字符串进行多次拼接
    println(name * 25)

//    3、通过 % 进行传值：
    printf("%d岁的%s，在尚硅谷学习大数据\n",age,name)

//    4、字符串模板：
    println(s"${age}岁的${name} ，在尚硅谷学习大数据")

//    5、格式化模板
    val num  = 23.45234
    println(f"the num is ${num}%1.3f")

//    6、 三引号： sql 常用
    val s =
      """
        |select
        |   *
        |from
        |   user;
        |""".stripMargin

    println(s)
  }
}
