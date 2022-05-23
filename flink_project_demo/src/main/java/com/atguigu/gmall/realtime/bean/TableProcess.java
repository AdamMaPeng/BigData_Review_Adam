package com.atguigu.gmall.realtime.bean;

import lombok.Data;

/**
 * @author Adam-Ma
 * @date 2022/5/22 13:43
 * @Project BigData_Review_Adam
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 * DESC : 配置 表对应的实体类
 */
@Data
public class TableProcess {
    //来源表
    String sourceTable;
    //输出表
    String sinkTable;
    //输出字段
    String sinkColumns;
    //主键字段
    String sinkPk;
    //建表扩展
    String sinkExtend;
}
