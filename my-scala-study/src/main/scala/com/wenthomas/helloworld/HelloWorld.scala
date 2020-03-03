package com.wenthomas.helloworld

/**
 * @author Verno
 * @create 2020-02-08 1:45 
 */
object HelloWorld {
  def main(args: Array[String]): Unit = {
    println("hello scala")
    val a = 10;
    println(a)
    val str = s"the input is $a"
    println(str)
    val row ="""
        |第一行
        |第二行
        |第三行
        |第四行
        |""".stripMargin
    println(row)
  }
}
