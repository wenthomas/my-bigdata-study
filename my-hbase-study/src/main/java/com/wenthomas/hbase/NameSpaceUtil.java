package com.wenthomas.hbase;

import jodd.util.CollectionUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Verno
 * @create 2020-02-07 16:22
 */

/*
 * 1. 创建/删除/查询/判断是否存在   名称空间
 *
 * 		hbase shell :  开启一个客户端对象
 * 		hbase shell : create_namespace 库名
 *
 * 2. Admin :  提供对hbase管理的一些api!
 * 			   例如创建，删除，查询表等！
 * 				可以使用Connection.getAdmin()来获取Admin的一个实例，使用完成后，调用
 * 				close关闭！
 *
 * 3. Connection：  Connection代表客户端和集群的一个连接！这个连接包含对master的连接，和zk的连接！
 * 				  Connection可以使用ConnectionFactory来创建！
 * 				  Connection的创建是重量级的，因此建议一个应用只创建一个Connection对象！Connection是线程安全的，
 * 				  可以在多个线程中共享同一个Connection实例！
 * 					Connection的生命周期也是用户自己控制！
 *
 * 				从Connection中获取Table和Admin对象的实例！Table和Admin对象的创建是轻量级，且不是线程安全的！
 * 				因此不建议池化或缓存Table和Admin对象的实例，每个线程有自己的Table和Admin对象的实例！
 *
 * 4. NamespaceDescriptor: 用来代表和定义名称空间
 */
public class NameSpaceUtil {
    private static Logger logger= LoggerFactory.getLogger(NameSpaceUtil.class);

    /**
     * 判断数据库是否存在
     * @param conn
     * @param nsname
     * @return
     */
    public static boolean ifNSExist(Connection conn, String nsname) throws IOException {
        //库名校验
        if (StringUtils.isBlank(nsname)) {
            logger.error("请输入正确的库名！");
            return false;
        }

        Admin admin = conn.getAdmin();

        //根据库名查询对应的NS，如果找不到就抛异常
        try {
            //提供一个Admin：用于实现库的管理
            NamespaceDescriptor namespaceDescriptor = admin.getNamespaceDescriptor(nsname);
            logger.info("NameSpace={}",namespaceDescriptor);
            return true;
        } catch (Exception e) {
            logger.info("找不到相应的数据库！");
            e.printStackTrace();
            return false;
        } finally {
            //关闭admin
            admin.close();
        }
    }

    /**
     * 查询所有的名称空间
     * @param conn
     * @return
     */
    public static List<String> listNameSpaces(Connection conn) throws IOException {
        List<String> nslist = new ArrayList<>();

        //提供一个admin对象
        Admin admin = conn.getAdmin();

        //查询所有的库
        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        //取出每个描述中库的名称集合
        for (NamespaceDescriptor descriptor : namespaceDescriptors) {
            nslist.add(descriptor.getName());
        }
        //关闭admin对象
        admin.close();

        return nslist;
    }

    /**
     * 查询库下有哪些表
     * @param conn
     * @param nsname
     * @return
     * @throws IOException
     */
    public static List<String> getTablesInNameSpace(Connection conn, String nsname) throws IOException {
        List<String> list = new ArrayList<>();

        //库名校验
        if (StringUtils.isBlank(nsname)) {
            logger.info("请正确输入数据库名！");
            return null;
        }

        //提供一个Admin对象
        Admin admin = conn.getAdmin();

        //查询当前库所有的表
        HTableDescriptor[] hTableDescriptors = admin.listTableDescriptorsByNamespace(nsname);
        for (HTableDescriptor tableDescriptor : hTableDescriptors) {
            list.add(tableDescriptor.getNameAsString());
        }

        //关闭admin
        admin.close();

        return list;
    }

    /**
     * 创建数据库
     * @param conn
     * @param nsname
     * @return
     * @throws IOException
     */
    public static boolean createNS(Connection conn, String nsname) throws IOException {
        //库名校验
        if (StringUtils.isBlank(nsname)) {

            logger.error("请输入正常的库名！");
            //在后台提示，库名非法
            return false;
        }

        //提供一个Admin
        Admin admin = conn.getAdmin();

        //创建数据库
        try {
            //先创建库的定义或描述
            NamespaceDescriptor descriptor = NamespaceDescriptor.create(nsname).build();
            admin.createNamespace(descriptor);
            return true;
        } catch (Exception e) {
            logger.error("创建数据库失败！");
            e.printStackTrace();
            return false;
        } finally {
            //关闭admin
            admin.close();
        }
    }

    /**
     * 删除数据库
     * @param conn
     * @param nsname
     * @return
     */
    public static boolean deleteNS(Connection conn, String nsname) throws IOException {
        //库名校验
        if (StringUtils.isBlank(nsname)) {

            logger.error("请输入正常的库名！");
            //在后台提示，库名非法
            return false;
        }

        //提供一个Admin
        Admin admin = conn.getAdmin();


        //只能删除空库，判断当前库是否为empty，不为空无法删除
        try {
            //查询当前库下有哪些表
            List<String> tablesInNameSpace = getTablesInNameSpace(conn, nsname);
            if (CollectionUtils.isEmpty(tablesInNameSpace)) {
                //空库则删除
                admin.deleteNamespace(nsname);

                return true;
            } else {
                logger.error(nsname+"库非空！无法删除！");

                return false;
            }
        } catch (Exception e) {
           logger.error("删除数据库失败！");
           e.printStackTrace();
           return false;
        } finally {
            //关闭Admin对象
            admin.close();
        }
    }
}
