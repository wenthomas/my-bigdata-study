# my-bigdata-study

大数据学习练习

## 一,Flume
### 1，自定义Source
[案例](src/main/java/com/wenthomas/flume/source/MySource.java)<br/>
[配置文件](src/main/resources/com/wenthomas/flume/source/)<br/>
### 2，自定义Interceptor
[案例](src/main/java/com/wenthomas/flume/interceptor/MyInterceptor.java)<br/>
[配置文件](src/main/resources/com/wenthomas/flume/interceptor/)<br/>
### 1，自定义Source
[案例](src/main/java/com/wenthomas/flume/sink/MySink.java)<br/>
[配置文件](src/main/resources/com/wenthomas/flume/sink/)<br/>

## 三,Hbase
引入依赖：
```xml
        <dependency>
            <groupId>org.apache.hbase</groupId>
            <artifactId>hbase-server</artifactId>
            <version>1.3.1</version>
        </dependency>

        <dependency>
            <groupId>org.apache.hbase</groupId>
            <artifactId>hbase-client</artifactId>
            <version>1.3.1</version>
        </dependency>

        <dependency>
            <groupId>jdk.tools</groupId>
            <artifactId>jdk.tools</artifactId>
            <version>1.8</version>
            <scope>system</scope>
            <systemPath>${JAVA_HOME}/lib/tools.jar</systemPath>
        </dependency>
```
备注：<br/>
在对HBase执行增删改查时，只需要引入hbase-client模块即可，运行MR操作hbase时，需要引入hbase-server。
拷贝hdfs-site.xml文件到客户端的类路径下！<br/>
hbase-site.xml
[配置文件](my-hbase-study/src/main/resources/hbase-site.xml)<br/>

### 0，获取Connection连接
ConnectionUtil.java
[案例](my-hbase-study/src/main/java/com/wenthomas/hbase/ConnectionUtil.java)<br/>
### 1，数据库操作
NameSpaceUtil.java
[案例](my-hbase-study/src/main/java/com/wenthomas/hbase/NameSpaceUtil.java)<br/>
### 2，数据库表操作
TableUtil.java
[案例](my-hbase-study/src/main/java/com/wenthomas/hbase/TableUtil.java)<br/>
### 3，数据操作
DataUtil.java
[案例](my-hbase-study/src/main/java/com/wenthomas/hbase/DataUtil.java)<br/>
### 4，hbase mr案例一：
[案例](my-hbase-study/src/main/java/com/wenthomas/hbase/mr1/)
MR读取hbase中t2表的部分数据写入到t4表中<br/>
### 5，hbase mr案例二：
[案例](my-hbase-study/src/main/java/com/wenthomas/hbase/mr2/)
MR读取HDFS中的数据并写入到hbase表中<br/>


## 四,离线数据仓库项目
### 1，日志生成器
[案例](my-log-collector/src/main/java/com/wenthomas/appclient/AppMain.java)<br/>
### 2，hive自定义日志解析函数
MyUDF.java[案例](my-hadoop-study/src/main/java/com/wenthomas/functions/MyUDF.java)<br/>
MyUDTF.java[案例](my-hadoop-study/src/main/java/com/wenthomas/functions/MyUDTF.java)<br/>