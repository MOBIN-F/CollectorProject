package com.mobin.collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by Mobin on 2017/6/22.
 * 校验程序的程序的启动参数
 */
public class CollectorOptions {
    private static final Logger log = LoggerFactory.getLogger(CollectorOptions.class);

    public int checkInterval = 2 * 60 * 1000;  //两分钟轮询一次
    public String dateTime = null;  //只采集某个小时的数据
    public String startTime = null;  //采集>=startTime的数据


}
