package com.atguigu.gmall.realtime.util;

/**
 * @author Adam-Ma
 * @date 2022/5/31 9:20
 * @Project BigData_Review_Adam
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 * DESC :   FlinkSQL 中 通过LookUp 查询缓存方式 读取 mysql 中数据的配置 工具类
 */
public class MysqlUtil {
    public static String mysqlLookUpTableDDL(String tableName){
        return  "WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://hadoop102:3306/gmall2022',\n" +
                "   'table-name' = '"+ tableName +"',\n" +
                "   'username' = 'root',\n" +
                "   'password' = 'root',\n" +
                "   'lookup.cache.max-rows' = '200',\n" +
                "   'lookup.cache.ttl' = '1 hour',\n" +
                "   'driver' = 'com.mysql.cj.jdbc.Driver'\n" +
                ")";
    }

    public static String getBaseDicLoopUpDDL(){
        return "create table base_dic (\n" +
                "  dic_code      string,\n" +
                "  dic_name      string,\n" +
                "  parent_code   string,\n" +
                "  create_time  timestamp,\n" +
                "  operate_time timestamp,\n" +
                "  primary key(dic_code) NOT ENFORCED\n" +
                ") " + mysqlLookUpTableDDL("base_dic");
    }
}
