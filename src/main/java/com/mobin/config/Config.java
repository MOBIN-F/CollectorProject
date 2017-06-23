package com.mobin.config;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Properties;

/**
 * Created by Mobin on 2017/6/17.
 * 所有采集源文件目录及目标目录的路径都在此统一配置管理
 */
public class Config {
    public static final String VERSION = "1.0.0";

    private static final Properties config = loadConfig();
    public static final String DATE_FORMAT = "yyyyMMdd";

    public static final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);

    public static final String mobin_file = getStringProperty("MobingetDateDir(dirs)_prefix");

    private static Properties loadConfig() {
        String confFile = System.getProperty("File_conf"); //可以在VM启动时配置该参数：-DFile_conf=路径
        InputStream in = null;
        if (confFile != null) {
            try {
                in = new FileInputStream(new File(confFile));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }

        if (in == null) {
            in = Config.class.getResourceAsStream("/File_conf.properties");
        }
        Properties properties = new Properties();
        try {
            properties.load(in);
            in.close();
        } catch (IOException e) {
           throw new RuntimeException("Failed to load File_conf.properties");
        }

        return properties;
    }

    //获取配置文件中的属性
    public static String getStringProperty(String key){
        String value = config.getProperty(key);
        if (value == null) {
            return null;
        }
        value = value.trim();
        return value;
    }
}
