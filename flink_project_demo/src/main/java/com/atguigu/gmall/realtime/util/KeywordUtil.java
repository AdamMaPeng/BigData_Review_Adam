package com.atguigu.gmall.realtime.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Adam-Ma
 * @date 2022/6/1 11:21
 * @Project BigData_Review_Adam
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 * DESC : IK 分词器工具类
 */
public class KeywordUtil {
    public static List analyze(String line){
        List<String> splitResult = new ArrayList<>();

        StringReader reader = new StringReader(line);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);

        Lexeme lexeme = null;
        try {
            while ((lexeme = ikSegmenter.next()) != null) {
                String word = lexeme.getLexemeText();
                splitResult.add(word);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return splitResult;
    }

    /**
     *  测试 ik 分词器
     * @param args
     */
    public static void main(String[] args) {
        String str = "Apple iPhone 12 (A2404) 64GB 白色 支持移动联通电信5G 双卡双待手机等3件商品";
        List analyze = analyze(str);
        System.out.println(analyze);
    }
}
