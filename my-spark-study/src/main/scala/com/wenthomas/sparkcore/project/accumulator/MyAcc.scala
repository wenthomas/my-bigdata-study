package com.wenthomas.sparkcore.project.accumulator

import com.wenthomas.sparkcore.project.bean.UserVisitAction
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 * @author Verno
 * @create 2020-03-19 15:57 
 */
/**
 * 自定义累加器：
 * 需要统计每个品类的点击量, 下单量和支付量,
 * 所以我们在累加器中使用 Map 来存储这些数据: Map(cid, “click”-> 100, cid, “order”-> 50, ….)
 */
class MyAcc extends AccumulatorV2[UserVisitAction, Map[(String, String), Long]] {

    //自身类型
    self =>

    private var map = Map[(String, String), Long]()

    override def isZero: Boolean = map.isEmpty

    override def copy(): AccumulatorV2[UserVisitAction, Map[(String, String), Long]] = {
        val acc = new MyAcc
        acc.map = map
        acc
    }

    /**
     * 重新 new 一个 map
     */
    override def reset(): Unit = {
        map = Map[(String, String), Long]()
    }

    override def add(v: UserVisitAction): Unit = {
        //分别计算三个不同指标
        //对于不用的行为做不同的处理：if或者模式匹配实现
        v match {

            case action if action.click_category_id != -1 =>
                //点击行为
                //(cid, "click") -> 100
                val key = (v.click_category_id.toString, "click")
                map += key -> (map.getOrElse(key, 0L) + 1L)
            case action if action.order_category_ids != "null" =>
                //下单行为
                //切出来的一个订单可以包含多个品类
                val order_category_ids = v.order_category_ids
                val cids = order_category_ids.split(",")
                cids.foreach( cid => {
                    val key = (cid, "order")
                    map += key -> (map.getOrElse(key, 0L) + 1L)
                })
            case action if action.pay_category_ids != "null" =>
                //支付行为
                //切出来的一个订单可以包含多个品类
                val pay_category_ids = v.pay_category_ids
                val cids = pay_category_ids.split(",")
                cids.foreach( cid => {
                    val key = (cid, "pay")
                    map += key -> (map.getOrElse(key, 0L) + 1L)
                })

            case _ =>
            //其他情况，不做任何处理，不能直接抛异常（考虑到存在脏数据的情况）
        }
    }

    override def merge(other: AccumulatorV2[UserVisitAction, Map[(String, String), Long]]): Unit = {
        // 如果是可变map, 则所有的变化都是在原集合中发生变化, 最后的值可以不用再一次添加
        // 如果是不变map, 则计算的结果, 必须重新赋值给原的map变量
        other match {
            case o: MyAcc =>
                var merge = self.map
                /**
                 * 注意：case模式匹配内的map不可修改，需要新建一个变量来接收迭代结果
                 */
                self.map ++= o.map.foldLeft(self.map)({
                    case (map, (cidAction, count)) =>
                        merge += cidAction -> (merge.getOrElse(cidAction, 0L) + count)
                        merge
                })


/*                o.map.foreach{
                    case ((cid, action), count) =>
                        self.map += (cid, action) -> (self.map.getOrElse((cid, action),0L) + count)
                }*/
            case _ =>
                throw new UnsupportedOperationException
        }
    }

    override def value: Map[(String, String), Long] = map

}
