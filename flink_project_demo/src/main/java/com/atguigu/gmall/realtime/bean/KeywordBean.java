package com.atguigu.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
/**
 * @author Adam-Ma
 * @date 2022/6/1 20:25
 * @Project BigData_Review_Adam
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 * DESC :关键词统计实体类
 *    由于 FlinkSQL 1.13 版本还没有提供将 table 中的数据直接写入到ClickHouse中，
 *    需要需要将 table 转换成 流，然后通过流的 addSink（JDBCSink） 方式将数据写入到 ClickHouse 中，
 *   table.toAppendStream(table,  Class) ： 将table 对象转换成流对象，需要流中的数据类型，所以提供了 KeyWord 对应的实体类
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordBean {
    // 窗口起始时间
    private String stt;
    // 窗口闭合时间
    private String edt;
    // 关键词来源
    private String source;
    // 关键词
    private String keyword;
    // 关键词出现频次
    private Long keyword_count;
    // 时间戳
    private Long ts;
}