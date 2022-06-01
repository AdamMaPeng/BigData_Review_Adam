package com.atguigu.gmall.realtime.app.func;

import com.atguigu.gmall.realtime.util.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @author Adam-Ma
 * @date 2022/6/1 14:14
 * @Project BigData_Review_Adam
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 * DESC : Flink SQL 中 可以使用的 用户自定义的 分词函数
 *   从官网中找到用户自定义的函数 ： UDTF ，user defined table function
 *   其中 Row 代表 经过处理后，返回的数据类型
 *   而 ROW<word STRING> 代表了 返回的 row 有多少列，每列都是什么类型
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String line) {
        List<String> keywords = KeywordUtil.analyze(line);
        for (String keyword: keywords) {
            // use collect(...) to emit a row
            collect(Row.of(keyword));
            System.out.println();
        }
    }
}
