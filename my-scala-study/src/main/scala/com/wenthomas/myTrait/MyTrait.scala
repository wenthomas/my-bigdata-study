package com.wenthomas.myTrait

/**
 * @author Verno
 * @create 2020-03-06 23:15 
 */
object MyTrait extends App {
    private val son = new Son
    son.func()
    private val grandson = new Grandson
    grandson.myfunc()
}

trait Father extends Grandpa {
    println("father构造器")
    override def func() = {
        println("father func")
    }
}

class Son extends Father {
    println("son构造器")
}

trait Grandpa {
    println("grandpa构造器")
    def func() = {
        println("grandpa func")
    }
}

trait Daughter {
    //自身类型：等价于 class Daughter extends Father
    _: Father =>
    def myfunc() = {
        println("daughter func")
        //自身对象调用Father的方法来使用
        this.func()
    }
}

class Grandson extends Daughter with Father {

}