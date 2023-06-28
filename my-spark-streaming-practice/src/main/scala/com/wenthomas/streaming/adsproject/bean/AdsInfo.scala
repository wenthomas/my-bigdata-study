package com.wenthomas.streaming.adsproject.bean

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

/**
 * @author Verno
 * @create 2020-03-24 22:21 
 */
case class AdsInfo(ts: Long,
                   area: String,
                   city: String,
                   userId: String,
                   adsId: String,
                   var timestamp: Timestamp = null,
                   var dayString: String = null, // 2019-12-18
                   var hmString: String = null) { // 11:20

    timestamp = new Timestamp(ts)

    val date = new Date(ts)
    dayString = new SimpleDateFormat("yyyy-MM-dd").format(date)
    hmString = new SimpleDateFormat("HH:mm").format(date)
}
