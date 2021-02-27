package com.codejiwei.gmall.realtime.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
    Author: codejiwei
    Date: 2021/2/26
    Desc:  IK分词器工具类
**/
public class KeywordUtilBak{
    public static List<String> analyze(String text){
        List<String> keywordList = new ArrayList<>();

        StringReader input = new StringReader(text);

        IKSegmenter ik = new IKSegmenter(input, true);
        Lexeme lex = null;

        while (true) {
            try {
                if ((lex = ik.next()) != null) {
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
        String text = "尚硅谷大数据培训";
        System.out.println(KeywordUtilBak.analyze(text));
    }

}
