package com.atguigu.scala.review.chapter01

/**
 *  @author Adam-Ma 
 *  @date 2022/4/28 14:46
 *  @Project BigData_Review_Adam
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*    定义一个伴生类，一个伴生对象
 *      伴生类中存在 的是非静态的属性
 *      伴生对象实则为一个 单例的对象，其中存在着 由 static 修饰的 变量
 *
 *      在 scala 中，实现与 java 同样的通过 构造器创建对象的方式，是
 *      在 半生类中： 类名(属性1:类型，属性2:类型)
 *
 *   在 scala 官网中，将没有伴生类的 object对象称之为 单例对象
 *                     有伴生类的 object 对象称之为 伴生对象
 *
 *       为了简化同时写 半生类和伴生对象 ，scala 提供了样例类 -- case class，
 *       case class 除了包含 伴生类 和伴生对象外，还提供了 apply（）方法---模式匹配特别好用
 *
 */
class Student(name:String, age: Int) {
  def printInfo() = {
    println(this.age + "岁的" + this.name + "在" + Student.school + "学习")
  }
}

object Student{
  var school = "尚硅谷"

  def main(args: Array[String]): Unit = {
    val s1 = new Student("Adam", 18)
    val s2 = new Student("Leo", 25)

    s1.printInfo()
    s2.printInfo()
  }
}
