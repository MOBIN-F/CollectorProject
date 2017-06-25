package com.mobin.collector;

import com.mobin.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

/**
 * Created by Mobin on 2017/6/25.
 */
public class DataCollector {

    private static final Logger log = LoggerFactory.getLogger(DataCollector.class);

    //目的：避免与Hadoop和Spark的日志混杂在一起
    public static synchronized void initLog4j(){
        String log4jFile = System.getProperty("log4j");
        InputStream in = null;
        //TODO 测试
        if (log4jFile != null) {
            try {
                in = new FileInputStream(new File(log4jFile));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }

        if (in ==null) {
            in = Config.class.getResourceAsStream("/log4j.properties");
        }

        Properties properties = new Properties();
        try {
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("日志配置加载完成");
    }


}
