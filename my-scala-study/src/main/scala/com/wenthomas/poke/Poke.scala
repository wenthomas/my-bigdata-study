package com.wenthomas.poke

/**
 * @author Verno
 * @create 2020-03-05 21:40 
 */
/**
 * 编写一个扑克牌只能有 4 种花色,让其 toString 方法分别返回各花色，并实现一个函数,检查某张牌的花色是否为红色
 */
/*
object Poke {

    def apply(color: String): Poke = new Poke(color)

    def main(args: Array[String]): Unit = {
        println(Poke.isRed(Poke("Spade")))
        println(Poke.isRed(Poke("Heart")))
        println(Poke.isRed(Poke("")))
        println(Poke.isRed(Poke("Diamond")))
    }

    def isRed(card: Poke): Boolean = {
        if (null == Poke) false
        if (card.color == "Heart" || card.color == "Diamond") true
        else false
    }
}

class Poke(val color: String) {
    override def toString: String = {this.color}
}*/

object Poke extends Enumeration {

    type Poke = Value
    val Spade = Value("♠")
    val Heart = Value("♥")
    val Club = Value("♣")
    val Diamond = Value("♦")

    override def toString(): String = {
        Poke.values.mkString(",")
    }

    def isRed(card: Poke): Boolean = {
        if (null == card) false
        card == Heart || card == Diamond
    }

    def main(args: Array[String]): Unit = {
        println(Poke.isRed(Poke.Spade))
        println(Poke.isRed(Poke.Heart))
        println(Poke.isRed(Poke.Club))
        println(Poke.isRed(Poke.Diamond))
    }
}
