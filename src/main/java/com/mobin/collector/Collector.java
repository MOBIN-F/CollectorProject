package com.mobin.collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;

/**
 * Created by Mobin on 2017/5/7.
 * 采集类
 */
public class Collector implements  Runnable {
    static final Logger log = LoggerFactory.getLogger(Collector.class);
    static final String DONE = ".done";
    static final String DOWN = ".down";
    String collectorPath;
    String srcPath;

    public static final String NEW_FILES = "_NEW_FILES_";
    public static final String _COPIED_FILES_ = "_COPIED_FILES_";

    static final FilenameFilter downFilter = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith("DOWN");
        }
    };

    @Override
    public void run() {

    }
}
