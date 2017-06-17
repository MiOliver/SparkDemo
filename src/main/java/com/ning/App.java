package com.ning;

import com.ning.common.SparkTest;
import com.ning.common.WordCount;

/**
 * Created by oliver on 2017/6/14.
 */
public class App {

    public static void main(String[] args){
//        SparkTest test=new SparkTest();
//        test.deal();
        WordCount wordCount=new WordCount();
        wordCount.run();

    }
}
