package com.wenthomas.collections

/**
 * @author Verno
 * @create 2020-03-09 18:34 
 */
object MySort extends App {

    val list1 = List(30, 50, 70, 60, 10, 20)

    val list2 = List(new User(10, "a"), new User(20, "b"),
        new User(20, "a"), new User(15, "c"))

    val list3 = List("zzzzz", "hello", "world", "hello", "aaa", "b")

    /**
     * sorted:
     */
    //1，比较一般数据：.sorted(Ordering.Int.reverse): 增加比较规则，传入Ordering比较器
    println(list1.sorted(Ordering.Int.reverse))

    //2，比较对象：传入隐式参数Ordering匿名内部类作为比较条件
    implicit val o = new Ordering[User]{
        override def compare(x: User, y: User): Int = {
            //根据age、name比较
            var r = x.age - y.age
            if (r == 0) r = x.name.compareTo(y.name)
            r
        }
    }
    println(list2.sorted)



    println("---------------------------------------------------------------------")
    /**
     * sortBy:最常用
     */

    //1，比较一般数据：.sortBy(指定比较规则，当为元组时表示多个比较条件)(传入Ordering比较器指定顺序,tuple长最大为9)
    println(list3.sortBy(x => x.length))
    println(list3.sortBy(x => (x.length,x))(Ordering.Tuple2(Ordering.Int.reverse, Ordering.String)))

    //2，比较对象
    println(list2.sortBy(x => (x.name, x.age))(Ordering.Tuple2(Ordering.String, Ordering.Int.reverse)))




    println("---------------------------------------------------------------------")
    /**
     * sortWith:
     * 直接传入一个类似compare的比较方法，返回Boolean值： <0 为升序， >0 为降序
     */
    println(list1.sortWith((x, y) => (x > y)))


}

class User(val age: Int, val name: String) extends Ordered[User] {

    //需要重写toString方便输出显示
    override def toString: String = s"[age = $age, name = $name]"

    //提供一个比较方法给.sorted
    override def compare(that: User): Int = this.age - that.age
}