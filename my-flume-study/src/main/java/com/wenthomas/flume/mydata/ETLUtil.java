package com.wenthomas.flume.mydata;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Verno
 * @create 2020-02-13 11:49
 */
public class ETLUtil {

    private static Logger logger= LoggerFactory.getLogger(ETLUtil.class);

    /**
     * 判断启动日志是否复合格式要求：验证JSON字符串的完整性，是否以{}开头结尾
     * 格式：{"en":"start"}
     * @param bodyStr
     * @return
     */
    public static boolean validStartLog(String bodyStr) {
        //1，判断body部分是否有数据
        if (StringUtils.isBlank(bodyStr)) {
            return false;
        }

        //2，去掉前后空格
        String trim = bodyStr.trim();

        //3，验证输入的JSON日志的完整性，不合法的返回false
        if (trim.startsWith("{") && trim.endsWith("}")) {
            return true;
        }
        return false;
    }

    /**
     * 判断事件日志是否复合格式要求:
     * 	 * 		事件日志：  时间戳|{}
     * 						时间戳需要合法：
     * 							a)长度合法(13位)
     * 							b)都是数字
     * 						验证JSON字符串的完整性，是否以{}开头结尾
     * @param bodyStr
     * @return
     */
    public static boolean validEventLog(String bodyStr) {
        //1，判断body部分是否有数据
        if (StringUtils.isBlank(bodyStr)) {
            return false;
        }

        //2，去掉前后空格
        String trim = bodyStr.trim();

        //3，将数据以“|”分割
        String[] words = trim.split("\\|");

        //3，验证输入的JSON日志的合法性，不合法的返回false

        //验证是否以 时间戳|日志 为格式
        if (words.length != 2) {
            return false;
        }

        //判断时间戳：是否13位 、是否全数字
        // isNumber()判断值是否是数值类型 123L 0xxx
        // isDigits() 判断字符串中是否只能是0-9的数字
        if (words[0].length() != 13 || !NumberUtils.isDigits(words[0])) {
            return false;
        }

        if (words[1].startsWith("{") && words[1].endsWith("}")) {
            return true;
        }

        return false;
    }

    public static void main(String[] args) {

        System.out.println(ETLUtil.validEventLog("1581514574072|{\"cm\":{\"ln\":\"-114.5\",\"sv\":\"V2.5.3\",\"os\":\"8.2.9\",\"g\":\"D83V3441@gmail.com\",\"mid\":\"48\",\"nw\":\"4G\",\"l\":\"en\",\"vc\":\"4\",\"hw\":\"750*1134\",\"ar\":\"MX\",\"uid\":\"48\",\"t\":\"1581505823964\",\"la\":\"-22.1\",\"md\":\"Huawei-17\",\"vn\":\"1.0.8\",\"ba\":\"Huawei\",\"sr\":\"G\"},\"ap\":\"app\",\"et\":[{\"ett\":\"1581479469330\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1581476410462\",\"action\":\"1\",\"type\":\"3\",\"content\":\"\"}},{\"ett\":\"1581510973353\",\"en\":\"active_foreground\",\"kv\":{\"access\":\"1\",\"push_id\":\"1\"}},{\"ett\":\"1581461666753\",\"en\":\"error\",\"kv\":{\"errorDetail\":\"at cn.lift.dfdfdf.control.CommandUtil.getInfo(CommandUtil.java:67)\\\\n at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\\\\n at java.lang.reflect.Method.invoke(Method.java:606)\\\\n\",\"errorBrief\":\"at cn.lift.appIn.control.CommandUtil.getInfo(CommandUtil.java:67)\"}},{\"ett\":\"1581487450933\",\"en\":\"comment\",\"kv\":{\"p_comment_id\":1,\"addtime\":\"1581421873108\",\"praise_count\":401,\"other_id\":2,\"comment_id\":6,\"reply_count\":8,\"userid\":7,\"content\":\"攀举误\"}},{\"ett\":\"1581479714214\",\"en\":\"praise\",\"kv\":{\"target_id\":7,\"id\":3,\"type\":3,\"add_time\":\"1581457214401\",\"userid\":6}}]}\n" +
                "{\"action\":\"1\",\"ar\":\"MX\",\"ba\":\"HTC\",\"detail\":\"\",\"en\":\"start\",\"entry\":\"3\",\"extend1\":\"\",\"g\":\"0RE94YJ5@gmail.com\",\"hw\":\"750*1134\",\"l\":\"pt\",\"la\":\"-52.4\",\"ln\":\"-81.1\",\"loading_time\":\"1\",\"md\":\"HTC-1\",\"mid\":\"49\",\"nw\":\"WIFI\",\"open_ad_type\":\"2\",\"os\":\"8.0.1\",\"sr\":\"R\",\"sv\":\"V2.9.3\",\"t\":\"1581483268450\",\"uid\":\"49\",\"vc\":\"10\",\"vn\":\"1.2.6\"}"));
    }
}
