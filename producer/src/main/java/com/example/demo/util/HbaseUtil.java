package com.example.demo.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class HbaseUtil {
    //获取链接
    private static Connection conn = null;
    //获取会话
    private static Admin admin = null;
    //获取配置
    private static Configuration conf = null;
    //获取配置文件
    private static Properties properties = new Properties();
    //初始化
    private static String hosts = HbaseUtil.getProperties("hosts") ;
    private static String zk_port = HbaseUtil.getProperties("zk_port");
    private static String hbase_master = HbaseUtil.getProperties("hbase_master");

    static {
         conf = HBaseConfiguration.create();
         conf.set("hbase.zookeeper.quorum", hosts);
         conf.set("hbase.zookeeper.property.clientPort", zk_port);
         conf.set("hbase.master", hbase_master);
    }

    /**
     * 获取链接对象
     * @return
     */
    public synchronized static  Connection getConnection(){
        try{
                conn = ConnectionFactory.createConnection(conf);
        }catch (Exception e){
            System.out.println("创建链接失败");
            e.printStackTrace();
        }
        return conn;
    }
    /**
     * 批量插入   rowkey: 客户号+产品号    columnFamily:列组 dayend (期末)  monthend （月末）
     */
    public static void insertTable(String columnFaimly, List<Map<String,String>> mapList) {
        Table T = null;
        String tableName = "reditpolicy";
        //存放put的list集合，用于批量插入
        List<Put> putList = new ArrayList<Put>();

        try{
            conn = HbaseUtil.getConnection();
            T = conn.getTable(TableName.valueOf(tableName));
            //mapList中的每一个valueMap都是一个vo对象
            for (int i = 0;i < mapList.size();i++){
               Map<String,String> valueMap = mapList.get(i);
               //获取rowkey:客户号_产品号
               String rowkey = valueMap.get("rowkey");
               Put put =new Put(rowkey.getBytes());
               //将rowkey从Map中删除
               valueMap.remove("rowkey");
               //此时map中只有指标的值
               for (Map.Entry<String,String> entry : valueMap.entrySet()){
                   put.addColumn(columnFaimly.getBytes(),entry.getKey().getBytes(),entry.getValue().getBytes());
               }
               putList.add(put);
            }
            T.put(putList);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                T.close();
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }
    /**
     * 查询
     */
    public static List<Map<String,String>> getList(String rowkey){

        Table T = null;
        String tableName = "reditpolicy";
        List<Map<String,String>> mapList = new ArrayList<Map<String,String>>();
        try {
            conn = HbaseUtil.getConnection();
            T = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowkey.getBytes());
            Result result = T.get(get);
            List<Cell> cellList = result.listCells();
            for (Cell cell : cellList){
                 Map<String,String> map = new HashMap<String,String>();
                 map.put("ruleId", Bytes.toString(CellUtil.cloneQualifier(cell)));
                 map.put("value", Bytes.toString(CellUtil.cloneValue(cell)));
                 mapList.add(map);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return mapList;
    }
    /**
     * 获取属性值
     * @param str
     * @return
     */
     public static String getProperties(String str){
         String value="";
         try{
//             InputStream fileInputStream = HbaseUtil.class.getClassLoader().getResourceAsStream("hbase.properties");
             properties.load(new FileInputStream("./hbase.properties"));
             value=properties.get(str).toString();
         }catch (Exception e){

            e.printStackTrace();
         }
         return value;
     }
    /**
     * 客户号反转+产品号
     */
    public static String ReserverStr(String customerNo, String produceId){
        StringBuilder stringBuilder = new StringBuilder(customerNo);
        String resCustomerNo = stringBuilder.reverse().toString();
        StringBuilder stringBuilder1 = new StringBuilder(resCustomerNo);
        String str = stringBuilder1.append("_"+produceId).toString();
        return str;
    }
}
