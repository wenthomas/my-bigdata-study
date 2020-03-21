package com.wenthomas.sparkpractice

import java.text.DecimalFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, MapType, StringType, StructField, StructType}

/**
 * @author Verno
 * @create 2020-03-21 3:35 
 */
object AreaClickUDAF extends UserDefinedAggregateFunction{
    // 输入数据的类型:  北京  String
    override def inputSchema: StructType = StructType(StructField("city_name", StringType)::Nil)

    // 缓存的数据的类型: 北京->1000, 天津->5000  Map,  总的点击量  1000/?
    override def bufferSchema: StructType = {
        StructType(StructField("city_count", MapType(StringType, LongType))::StructField("total_count", LongType)::Nil)
    }

    // 输出的数据类型  "北京21.2%，天津13.2%，其他65.6%"  String
    override def dataType: DataType = StringType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = Map[String, Long]()
        buffer(1) = 0L
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        buffer(1) = buffer.getLong(1) + 1L

        val map = buffer.getAs[Map[String, Long]](0)
        val key = input.getString(0)
        buffer(0) = map + (key -> (map.getOrElse(key, 0L) + 1L))

    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        val map1 = buffer1.getAs[Map[String, Long]](0)
        val map2 = buffer2.getAs[Map[String, Long]](0)

        buffer1(0) = map2.foldLeft(map1)({
            case (map, (cityName, count)) =>
                map + (cityName -> (map.getOrElse(cityName, 0L) + count))
        })
        buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    override def evaluate(buffer: Row): Any = {
        val cityCountMap = buffer.getAs[Map[String, Long]](0)
        val totalCount = buffer.getLong(1)

        //对cityCountMap做排序筛选
        val remarkList = cityCountMap.toList.sortBy(-_._2)

        val formatted = new DecimalFormat(".00%")

        //分情况输出
        if (remarkList.size > 2) {
            val restList = remarkList.take(2)
            val cityList = restList.map({
                case (cityName, clickCount) =>
                    val rate = clickCount.toDouble / totalCount
                    cityName + ":" + formatted.format(rate)
            })
            cityList.mkString(", ") + ", 其他:" + formatted.format(remarkList.tail.tail.map(_._2).sum.toDouble / totalCount)
        } else {
            val cityList = remarkList.map({
                case (cityName, clickCount) =>
                    val rate = clickCount.toDouble / totalCount
                    cityName + ":" + formatted.format(rate)
            })
            cityList.mkString(", ")
        }
    }
}

/*case class CityRemark(cityName: String, cityRadio: Double) {
    new DecimalFormat()
}*/
