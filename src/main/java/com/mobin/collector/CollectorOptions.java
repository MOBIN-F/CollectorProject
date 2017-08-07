package com.mobin.collector;

import com.mobin.config.Config;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.nativeio.Errno;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;


/**
 * Created by Mobin on 2017/6/22.
 * 校验程序的程序的启动参数
 */
public class CollectorOptions {
    private static final Logger log = LoggerFactory.getLogger(CollectorOptions.class);

    public int checkInterval = 2 * 60 * 1000;  //两分钟轮询一次
    public String dateTime = null;  //只采集某个小时的数据
    public String startTime = null;  //采集>=startTime的数据

    boolean parallelizable = false;

    private final HashSet<String> collectors = new HashSet<>();  //要采集哪种类型的数据

    public CollectorOptions(){}

    public CollectorOptions(String[] args) {
        String collectorsStr = null;
        for (int i = 0; i < args.length; i ++) {
            String a = args[i];
            switch (a) {
                case "-dateTime":
                    dateTime = args[++ i];
                    break;
                case "-startTime":
                    startTime = args[++ i];
                    break;
                case "-collectors":
                    collectorsStr = args[++ i];
                    break;
                case "-parallelizable":
                    parallelizable = Boolean.parseBoolean(args[++i]);
                    break;
                case "-checkInterval":
                    checkInterval = Integer.parseInt(args[++ i]) * 1000;
                    break;
                default:
                    log.error("无效的参数" + a);
                    System.exit(-1);
            }
        }

        if (dateTime != null && startTime != null) {
            log.error("-dateTime和startTime只能同时指定其中一个");
        }

        if (dateTime ==null && startTime ==null) {
            startTime = FSUtils.formateDate(new Date(), Config.dateFormat);
        }

        checkDateTime(dateTime);
        checkDateTime(startTime);
        collectors.addAll(Arrays.asList(collectorsStr.split(",")));
    }

    public ArrayList<Collector> createCollectors(FileSystem fs) {
        ArrayList<Collector> collectorList = new ArrayList<>();
        if (collectors.contains("mobin")) {
            MobinFileCollector c = new MobinFileCollector();
            addCollector(fs, c, collectorList);
        }
        return collectorList;
    }

    public void addCollector(FileSystem fs , Collector collector, ArrayList<Collector> collectorList){
        collectorList.add(collector);
        collector.fs = fs;
        collector.options = this;
    }

    private void checkDateTime(String dateTime) {
        if (dateTime ==null) {
            return;
        }
        if (dateTime.length()  < 10) {
            log.error("指定的时间"+dateTime+"格式不对,正确的格式为："  + Config.DATE_TIME_FORMAT);
        }

        try {
            FSUtils.parseDate(dateTime, Config.dateTimeFormat);
        } catch (ParseException e) {
            log.error("无效的日期时间" + dateTime + "," + e);
            System.exit(-1);
        }
    }

    @Override
    public String toString() {
        return "CollectiorOptions [dateTime= "+ dateTime +", startTime="+ startTime +"," +
                "collectors="+ collectors+", checkInterval="+checkInterval+"]";
    }
}
