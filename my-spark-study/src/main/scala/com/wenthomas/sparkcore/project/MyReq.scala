package com.wenthomas.sparkcore.project

import java.text.DecimalFormat

import com.wenthomas.sparkcore.project.accumulator.MyAcc
import com.wenthomas.sparkcore.project.bean.{CategoryCountInfo, SessionInfo, UserVisitAction}
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * @author Verno
 * @create 2020-03-18 3:46 
 */
/**
 * 日志数据：
 *  2019-07-17_95_26070e87-1ad7-49a3-8fb3-cc741facaddf_37_2019-07-17 00:00:02_手机_-1_-1_null_null_null_null_3
 */
object MyReq {



    /**
     * 需求一： Top10 热门品类
     *
     * @param sc
     * @param userVisitActionRDD
     */
    def request1(sc: SparkContext, userVisitActionRDD: RDD[UserVisitAction]) = {
        //注册累加器
        val acc = new MyAcc
        sc.register(acc, "myAcc")

        userVisitActionRDD.foreach(acc.add(_))

        // 1. 把一个品类的三个指标封装到一个map中
        val cidActionCountGrouped = acc.value.groupBy(map => map._1._1)

        // 2. 把结果封装到样例类中
        val categoryCountInfoArray = cidActionCountGrouped.map({
            case (cid, map) =>
                CategoryCountInfo(
                    cid,
                    map.getOrElse((cid, "click"), 0L),
                    map.getOrElse((cid, "order"), 0L),
                    map.getOrElse((cid, "pay"), 0L)
                )
        }).toArray

        // 3. 对数据进行排序取top10
        val result = categoryCountInfoArray
                .sortBy(info => (-info.clickCount, -info.orderCount, -info.payCount))
                .take(10)

        result.foreach(println)

        result
    }


    /**
     * 需求2: top10品类的top10session
     * 方案一：分组，每组内排序取前10
     *  组内排序，使用scala排序，需要把一组内所有数据加载到内存，有可能造成内存溢出
     * @param sc
     * @param categoryTop10
     * @param userVisitActionRDD
     * @return
     */
    def statCategorySessionTop10_1(sc: SparkContext, categoryTop10: Array[CategoryCountInfo], userVisitActionRDD: RDD[UserVisitAction]) = {
        //1，在总数据中过滤出只包含top10品类的点击记录
        val cids = categoryTop10.map(_.categoryId.toLong)
        val filteredUserVisitActionRDD = userVisitActionRDD.filter(x => cids.contains(x.click_category_id))

        //2，每个品类top10的session获取
        val cidSidAndOne = filteredUserVisitActionRDD.map(x => ((x.click_category_id, x.session_id), 1))

        val cidSidAndCount = cidSidAndOne.reduceByKey(_ + _)

        val cidAndSidCount = cidSidAndCount.map(x => (x._1._1, (x._1._2, x._2)))

        val cidAndSidCountItRDD = cidAndSidCount.groupByKey()

        // 只能使用scala的排序, scala排序必须把所有数据全部加装内存才能排
        val result = cidAndSidCountItRDD.mapValues(it => it.toList.sortBy(-_._2).take(10))

        result.foreach(println)

        result
    }


    /**
     * 需求2: top10品类的top10session
     * 方案二：每次排序一个cid, 需要排10次
     * @param sc
     * @param categoryTop10
     * @param userVisitActionRDD
     * @return
     */
    def statCategorySessionTop10_2(sc: SparkContext, categoryTop10: Array[CategoryCountInfo], userVisitActionRDD: RDD[UserVisitAction]) = {
        //1，在总数据中过滤出只包含top10品类的点击记录
        val cids = categoryTop10.map(_.categoryId.toLong)
        val filteredUserVisitActionRDD = userVisitActionRDD.filter(x => cids.contains(x.click_category_id))

        //2，对每个cid的数据进行聚合排序
        val temp = cids.map(cid => {
            //过滤出点击id是cid的记录
            val cidUserVisitActionRDD = filteredUserVisitActionRDD.filter(x => x.click_category_id == cid)
            //聚合
            val result = cidUserVisitActionRDD
                    .map(action => ((action.click_category_id, action.session_id), 1))
                    .reduceByKey(_ + _)
                    .map({
                        case ((cid, sid), count) => (cid, (sid, count))
                    })
                    .sortBy(_._2._2)
                    .take(10)
                    .groupBy(_._1)
                    .map({
                        case (cid, arr) => (cid, arr.map(_._2).toList)
                    })
            result
        })
        val result = temp.flatMap(map => map)
        result.foreach(println)
        result
    }


    /**
     * 需求2: top10品类的top10session
     * 方案三：基于方案一找一个可以排序的集合, 然后时刻保持这个集合中只有10个最大的元素.
     *  解决了直接在iterator中排序，不会再造成内存溢出
     * @param sc
     * @param categoryTop10
     * @param userVisitActionRDD
     * @return
     */
    def statCategorySessionTop10_3(sc: SparkContext, categoryTop10: Array[CategoryCountInfo], userVisitActionRDD: RDD[UserVisitAction]) = {
        //1，在总数据中过滤出只包含top10品类的点击记录
        val cids = categoryTop10.map(_.categoryId.toLong)
        val filteredUserVisitActionRDD = userVisitActionRDD.filter(x => cids.contains(x.click_category_id))

        //2，每个品类top10的session获取
        val cidSidAndOne = filteredUserVisitActionRDD.map(x => ((x.click_category_id, x.session_id), 1))

        val cidSidAndCount = cidSidAndOne.reduceByKey(_ + _)

        val cidAndSidCount = cidSidAndCount.map(x => (x._1._1, (x._1._2, x._2)))

        val cidAndSidCountItRDD = cidAndSidCount.groupByKey()

        // 对每个value进行组内排序取前10
        val result = cidAndSidCountItRDD.mapValues(it => {
            //不要把iterator直接转成List排序
            var set = mutable.TreeSet[SessionInfo]()
            it.foreach({
                case (sid, count) => {
                    //将数据封装入自定义对象，在通过set自动排序取前10
                    val info = SessionInfo(sid, count)
                    set += info
                    if (set.size > 10) set = set.take(10)
                }
            })
            set.toList
        })

        result.foreach(println)

        result
    }

    /**
     * 需求2: top10品类的top10session
     * 方案四：将reduceByKey 和 groupByKey两次shuffle减少为一次shuffle
     *  通过自定义分区器，在reduceByKey实现分区操作
     * @param sc
     * @param categoryTop10
     * @param userVisitActionRDD
     * @return
     */
    def statCategorySessionTop10_4(sc: SparkContext, categoryTop10: Array[CategoryCountInfo], userVisitActionRDD: RDD[UserVisitAction]) = {
        //1，在总数据中过滤出只包含top10品类的点击记录
        val cids = categoryTop10.map(_.categoryId.toLong)
        val filteredUserVisitActionRDD = userVisitActionRDD.filter(x => cids.contains(x.click_category_id))

        //2，每个品类top10的session获取
        val cidSidAndOne = filteredUserVisitActionRDD.map(x => ((x.click_category_id, x.session_id), 1))

        // 使用自定义分区器实现分组聚合
        val cidSidAndCount = cidSidAndOne.reduceByKey(new CategorySessionPartitioner(cids), _ + _)

        //3，cidSidAndCount 中执行mapPartitions

        val result = cidSidAndCount.mapPartitions(it => {
            // 不要把iterable直接转成list再排序
            var set = mutable.TreeSet[SessionInfo]()
            var categoryId = -1L
            it.foreach({
                case ((cid, sid), count) =>
                    categoryId = cid
                    val info = SessionInfo(sid, count)
                    set += info
                    if (set.size > 10) set = set.take(10)
            })
            Iterator((categoryId, set.toList))
        })

        result.foreach(println)

        result
    }

    class CategorySessionPartitioner(cids: Array[Long]) extends Partitioner {

        // 建立一个hash表，使得每个值的hash在集合内唯一（通过与索引建立映射一一对应）
        private val cidIndexMap = cids.zipWithIndex.toMap

        // 分区和品类ID数量保持一致，可以保证一个分区只有一个cid，此处分为了10个分区
        override def numPartitions: Int = cids.length

        override def getPartition(key: Any): Int = {
            key match {
                //使用这个cid在数组中的下标作为分区的索引，根据传入的cid查找分区
                case (cid: Long, _) => cidIndexMap(cid)
            }
        }
    }




    /**
     * 需求三：页面单跳转化率统计
     * 思路：
     * 1，先求出每个目标页面的点击量
     * 2，通过拉链的方式求出用户 from -> to 页面跳转信息，再分组聚合求出各个跳转页面的次数
     * 3，计算跳转率
     *
     * @param sc
     * @param userVisitActionRDD
     * @param pageString
     * @return
     */
    def statPageConversionRate(sc: SparkContext, userVisitActionRDD: RDD[UserVisitAction], pageString: String) = {
        //1，做出目标跳转流 1,2,3,4,5,6,7,
        val pages = pageString.split(",")
        val targetPagesFlows = pages.zip(pages.tail).map({
            case (from, to) => s"$from->$to"
        })

        //1.1，把targetPagesFlows做成广播变量，优化性能
        val targetPagesFlowBC = sc.broadcast(targetPagesFlows)

        //2，计算分母，计算目标页面的点击量
        val pageAndCount = userVisitActionRDD
                .filter(action => pages.take(pages.length - 1).contains(action.page_id.toString))
                .map(action => (action.page_id, 1))
                .countByKey()

        //3，计算分子
        //3.1，按照sessionId分组，不能先对页面做过滤，否则会打乱原本的页面点击逻辑
        val sessionIdGrouped = userVisitActionRDD.groupBy(action => action.session_id)
        //flatMap：将List扁平化为String
        val pageFlowRDD = sessionIdGrouped.flatMap({
            case (sid, actionIt) =>
                //先按时间排好序
                val actions = actionIt.toList.sortBy(_.action_time)
                actions
                        .zip(actions.tail)
                        .map({
                            case (fromAction, toAction) => s"${fromAction.page_id}->${toAction.page_id}"
                        })
                        .filter(flow => targetPagesFlowBC.value.contains(flow))
        })
        //3.2，聚合
        val pageFlowsAndCount = pageFlowRDD.map((_, 1)).countByKey()


        val formatted = new DecimalFormat(".00%")
        //4，计算跳转率
        val result = pageFlowsAndCount.map({
            case (flow, count) =>
                val rate = count.toDouble / pageAndCount(flow.split("->")(0).toLong)
                (flow, formatted.format(rate))
        })

        println(result)
        result
    }
}



