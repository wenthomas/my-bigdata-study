package com.wenthomas.functions;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Verno
 * @create 2020-02-16 16:03
 */

/*
 * 1. 继承 GenericUDTF
 * 		initialize
 *
 * 2. 传入的参数：　[{},{},{}]
 */
public class MyUDTF extends GenericUDTF {

    private static Logger logger= LoggerFactory.getLogger(MyUDTF.class);

    // 在函数运行之前被调用一次，作用是告诉MapTask,当前函数返回的结果类型和个数，以便MapTask在运行时，
    // 会函数的返回值进行检查
    // 当前此函数，应该在initialize声明返回值类型是 2列String类型的数据！
    // Inspector：检查员
    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        //初始化当前返回的两列字段别名
        List<String> fieldNames = new ArrayList<>();

        fieldNames.add("event_name");
        fieldNames.add("event_json");

        //初始化当前返回的两列的类型检查器
        List<ObjectInspector> fieldOIs = new ArrayList<>();

        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        //返回构建好的函数检察员
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    //执行函数的功能，处理数据后调用forward()返回结果
    // 返回的应该是2列Ｎ行的数据，
    @Override
    public void process(Object[] args) throws HiveException {
        //检查是否传入了参数
        if (args[0]==null || args.length==0) {
            return ;
        }

        try {
            //构建JSON数组对象
            JSONArray jsonArray = new JSONArray(args[0].toString());

            //构建JSON数据组对象是否失败
            if (null == jsonArray || jsonArray.length() == 0) {
                return;
            }

            //取出JSON数组，再从每一个{}中取出事件名
            for (int i = 0; i < jsonArray.length(); i++) {
                try {
                    //每遍历一次，需要写出去一行两列的数据，构建一个数组，存储一行两列的数据
                    String [] result=new String [2];

                    JSONObject jsonObject = jsonArray.getJSONObject(i);

                    //取出当中的事件名
                    result[0]=jsonObject.getString("en");

                    //将整个当前jsonstr作为第二列
                    result[1]=jsonObject.toString();

                    //System.out.println(Arrays.asList(result));

                    //将result写出
                    forward(result);
                } catch (Exception e) {

                    //继续开始遍历下一个{}
                    continue;

                }
            }



        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    // 在调用函数完成之后,执行一些清理或关闭操作
    @Override
    public void close() throws HiveException {

    }

    public static void main(String[] args) throws JSONException, HiveException {
        String param = "1581684648834|{\"cm\":{\"ln\":\"-119.4\",\"sv\":\"V2.3.6\",\"os\":\"8.0.5\",\"g\":\"6Q01A9X9@gmail.com\",\"mid\":\"9\",\"nw\":\"4G\",\"l\":\"es\",\"vc\":\"9\",\"hw\":\"750*1134\",\"ar\":\"MX\",\"uid\":\"9\",\"t\":\"1581593982346\",\"la\":\"-56.3\",\"md\":\"HTC-10\",\"vn\":\"1.1.0\",\"ba\":\"HTC\",\"sr\":\"R\"},\"ap\":\"app\",\"et\":[{\"ett\":\"1581625010695\",\"en\":\"newsdetail\",\"kv\":{\"entry\":\"1\",\"goodsid\":\"3\",\"news_staytime\":\"2\",\"loading_time\":\"15\",\"action\":\"2\",\"showtype\":\"3\",\"category\":\"36\",\"type1\":\"\"}},{\"ett\":\"1581648141182\",\"en\":\"ad\",\"kv\":{\"entry\":\"1\",\"show_style\":\"3\",\"action\":\"3\",\"detail\":\"325\",\"source\":\"4\",\"behavior\":\"1\",\"content\":\"2\",\"newstype\":\"8\"}},{\"ett\":\"1581630048422\",\"en\":\"active_foreground\",\"kv\":{\"access\":\"1\",\"push_id\":\"2\"}},{\"ett\":\"1581618434873\",\"en\":\"error\",\"kv\":{\"errorDetail\":\"java.lang.NullPointerException\\\\n    at cn.lift.appIn.web.AbstractBaseController.validInbound(AbstractBaseController.java:72)\\\\n at cn.lift.dfdf.web.AbstractBaseController.validInbound\",\"errorBrief\":\"at cn.lift.appIn.control.CommandUtil.getInfo(CommandUtil.java:67)\"}},{\"ett\":\"1581649791440\",\"en\":\"comment\",\"kv\":{\"p_comment_id\":2,\"addtime\":\"1581673321403\",\"praise_count\":540,\"other_id\":4,\"comment_id\":6,\"reply_count\":183,\"userid\":5,\"content\":\"骚铣秸壬涣煌愧臻例妇则涛懒稽趣炉\"}}]}";

        Object[] array = new Object[2];
        MyUDTF myUDTF = new MyUDTF();
        MyUDF myUDF = new MyUDF();

        array[0] = myUDF.evaluate(param, "et");
        logger.info("et={}", array[0]);

        myUDTF.process(array);
    }
}
