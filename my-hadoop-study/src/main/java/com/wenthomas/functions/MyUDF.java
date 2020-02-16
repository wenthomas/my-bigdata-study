package com.wenthomas.functions;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Verno
 * @create 2020-02-16 15:46
 */

/*
 * 1.继承 UDF类，提供多个evaluate方法，evaluate不能返回void，但是可以返回null
 *
 * ①取ap属性的值app:   a(jsonstr,'ap')=app
②取json的1581472611770：   a(jsonstr,'ts')=1581472611770
③取cm公共部分中的属性，例如：
	取cm中的ln的值：  a(jsonstr,'ln')=-45.5
	取cm中的sv的值：  a(jsonstr,'sv')=V2.7.9
	取et:  a(jsonstr,'et')=[{},{},{}]

	param应该包含ts！

	数据格式：  时间戳 | {"ap":"","cm":{},"et":[{},{}]}
 */
public class MyUDF extends UDF {

    private static Logger logger= LoggerFactory.getLogger(MyUDF.class);

    public String evaluate(String source, String param) throws JSONException {
        //1，参数检查：source中，没有Param，也不是"ts"，则返回空串查询失败
        if (!source.contains(param) && !"ts".equals(param)) {
            return "";
        }

        //2，将数据按照“|”分割
        String[] words = source.split("\\|");

        //3，构建JSON对象
        JSONObject root = new JSONObject(words[1]);

        //取出时间戳字段
        if ("ts".equals(param)) {
            return words[0].trim();
        } else if ("ap".equals(param)) {
            return root.getString("ap");
        } else if ("et".equals(param)) {
            return root.getString("et");
        } else {
            //取出的是cm中的某个属性
            return root.getJSONObject("cm").getString(param);
        }
    }

    public static void main(String[] args) throws JSONException {
        String line = "1541217850324|{\"cm\":{\"mid\":\"m7856\",\"uid\":\"u8739\",\"ln\":\"-74.8\",\"sv\":\"V2.2.2\",\"os\":\"8.1.3\",\"g\":\"P7XC9126@gmail.com\",\"nw\":\"3G\",\"l\":\"es\",\"vc\":\"6\",\"hw\":\"640*960\",\"ar\":\"MX\",\"t\":\"1541204134250\",\"la\":\"-31.7\",\"md\":\"huawei-17\",\"vn\":\"1.1.2\",\"sr\":\"O\",\"ba\":\"Huawei\"},\"ap\":\"weather\",\"et\":[{\"ett\":\"1541146624055\",\"en\":\"display\",\"kv\":{\"goodsid\":\"n4195\",\"copyright\":\"ESPN\",\"content_provider\":\"CNN\",\"extend2\":\"5\",\"action\":\"2\",\"extend1\":\"2\",\"place\":\"3\",\"showtype\":\"2\",\"category\":\"72\",\"newstype\":\"5\"}},{\"ett\":\"1541213331817\",\"en\":\"loading\",\"kv\":{\"extend2\":\"\",\"loading_time\":\"15\",\"action\":\"3\",\"extend1\":\"\",\"type1\":\"\",\"type\":\"3\",\"loading_way\":\"1\"}},{\"ett\":\"1541126195645\",\"en\":\"ad\",\"kv\":{\"entry\":\"3\",\"show_style\":\"0\",\"action\":\"2\",\"detail\":\"325\",\"source\":\"4\",\"behavior\":\"2\",\"content\":\"1\",\"newstype\":\"5\"}},{\"ett\":\"1541202678812\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1541184614380\",\"action\":\"3\",\"type\":\"4\",\"content\":\"\"}},{\"ett\":\"1541194686688\",\"en\":\"active_background\",\"kv\":{\"active_source\":\"3\"}}]}";

        String result = new MyUDF().evaluate(line, "uid");

        logger.info(result);
    }
}
