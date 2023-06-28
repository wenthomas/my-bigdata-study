package com.wenthomas.genericity

/**
 * @author Verno
 * @create 2020-03-10 14:34 
 */
/**
 *  泛型及隐式转换的应用：
 *  让max方法可以比较一个普通的样例类对象：需要样例类有一个隐式参数实现了Ordering的比较方法
 */
object MyGenericity extends App {

    println(max(10, 20))
    private val user1: User = User("wenthomas", 10)
    private val user2: User = User("Verno", 20)
    println(max(user1, user2))

    def max[T: Ordering](x: T, y:T) = {
        //从冥界召唤隐式值
        val ord = implicitly[Ordering[T]]

        if (ord.gt(x, y)) x else y
    }

}

case class User(name: String, age: Int)
object User{
    implicit val ord: Ordering[User] = new Ordering[User]() {
        override def compare(x: User, y: User): Int = x.age - y.age
    }
}
