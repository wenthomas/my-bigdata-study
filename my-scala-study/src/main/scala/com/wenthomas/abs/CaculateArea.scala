package com.wenthomas.abs

/**
 * @author Verno
 * @create 2020-03-05 22:58 
 */
/**
 * 定义一个抽象类 Shape，一个抽象方法 centerPoint，以及该抽象类的子类 Rectangle 和 Circle。
 * 为子类提供合适的构造器，并重写centerPoint方法, 并提供计算面积的方法 (根据需要添加相应的属性)
 */
object CaculateArea {
    def main(args: Array[String]): Unit = {
        val rectangle = new Rectangle(10)
        println(rectangle.centerPoint)
        val circle = new Circle(10)
        println(circle.centerPoint)
    }
}

abstract class Shape {
    var arg: Double = _
    var area: Double = _
    def centerPoint(): Double
}

class Rectangle extends Shape {
    def this(arg: Double) {
        this()
        this.arg = arg
    }
    override def centerPoint(): Double = {
        this.area = this.arg * this.arg
        this.area
    }
}

class Circle extends Shape {
    def this(arg: Double) {
        this()
        this.arg = arg
    }
    override def centerPoint(): Double = {
        this.area = this.arg * this.arg * Math.PI
        this.area
    }
}