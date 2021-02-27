package com.codejiwei.gmall.realtime.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Author: codejiwei
 * Date: 2021/2/26
 * Desc: IK分词工具类
 **/
public class KeywordUtil {
    public static List<String> analyze(String text) {

        //提供IK分词器需要传入的Reader对象，用来封装text
        StringReader input = new StringReader(text);
        //创建IK分词器对象，其中第二个参数是是否使用智能分词
        IKSegmenter ik = new IKSegmenter(input, true);
        //创建用来存储关键字的集合，作为方法输出
        List<String> keywordList = new ArrayList<>();
        Lexeme lex = null;
        while (true){
            try {

                if ((lex = ik.next()) != null){
                    String lexemeText = lex.getLexemeText();
                    keywordList.add(lexemeText);
                }else {
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return keywordList;
    }

    public static void main(String[] args) {
        String text = "Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待";
        System.out.println(KeywordUtil.analyze(text));

    }
}
