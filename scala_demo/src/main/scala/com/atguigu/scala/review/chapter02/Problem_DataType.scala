package com.atguigu.scala.review.chapter02

/**
 *  @author Adam-Ma 
 *  @date 2022/4/29 16:34
 *  @Project BigData_Review_Adam
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
 *      明确几个常识：
 *        1、计算机底层的计算逻辑是不变的： 针对二进制数进行加法运算
 *        2、针对 Scala来说， Int 用 4 byte 表示 ，Byte 用 1 个byte 表示
 *        3、计算机底层, 最高位为 0 表示正数， 最高位为 1 表示负数
 *
 *      问题： 为什么计算机底层使用二进制补码表示？ 为什么 Byte 的范围为 -128 - 127 ？
 *        问题一： 为什么计算机底层使用二进制补码表示？
 *            -1： 原码：   1000 0001
 *            -2： 原码：   1000 0010
 *            -127 原码：   1111 1111
 *          1）因为计算机底层的计算逻辑是一致的，都是针对二进制进行加法运算。例如 -2 + 1 = -1 ，当我们以原码进行计算时，
 *             -2原码：1000 0010 + 0000 0001 = 1000 0011 ！= -1原码：1000 0001
 *            -1： 反码：   1111 1110
 *            -2： 反码：   1111 1101
 *            -127 反码：   1000 0000
 *          2）当引入了 反码 ，当 -2 + 1 用反码进行表示的时候，就能保证计算机底层的计算逻辑是一致的
 *             -2 反码：1111 1101 + 0000 0001 = -1反码：1111 1110
 *          3）虽然反码计算，解决了计算机底层计算的逻辑一致性问题，也无法解决 0 表示的问题，
 *             对于用正数表示来说 ： 0000 0000
 *             -1反码 + 1 = 0 ，用反码表示为：-1反码：1111 1110 + 0000 0001 = 1111 1111
 *          4） 此时对于 0的表示， 就有了 ：
 *              +0 ： 0000 0000
 *              -0 ： 1111 1111
 *          5） 为了解决这个问题，就引入了 补码的概念，在反码的基础上 + 1 ,
 *             -1： 补码：   1111 1111
 *             -2： 补码：   1111 1110
 *             -127 补码：   1000 0001
 *          6） 此时 -1补码 + 1 =   1111 1111 + 0000 0001 = 00001 0000 0000  --》 舍去最高位得 ： 0000 0000
 *          7） 此时就解决了 两种表示 0 的方式，打通了正数 和 负数运算的一致性
 *        问题二：为什么 Byte 的范围为 -128 - 127 ？
 *          8） 此时 对于正数 ，最大为： 0111 1111 = 127 ，
 *                  对于负数， 最大为 ：1000 0001 = -127
 *          9） 多余一个 1000 0000 ，这个数依然是负数，但是不能表示 -0 ，所以就表示了 -128
 *
 *      面试题：
 *        val n : Int = 130
 *        val b : Byte = n.toByte
 *        println(b)
 *      答案： -126
 *
 *      解题思路： 有了以上的问题解析，再来考虑这个面试题
 *        n : Int = 130
 *          补码： 0000 0000 |0000 0000 |0000 0000 |1000 0010
 *        n.toByte:
 *          补码： 1000 0010
 *          原码： 1111 1110  --> -126
 */
object Problem_DataType {
  def main(args: Array[String]): Unit = {
    // 面试题
    val n : Int = 130
    val b : Byte = n.toByte
    println(b)
  }
}
