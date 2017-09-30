package com.mobin.common;

import com.mobin.config.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

/**
 * Created by Mobin on 2017/9/29.
 */
public class DataPartitioner implements Serializable, Partitioner{
    public static final String PARTITIONED = ".partitioned";
    @Override
    public void createOrAppendPartitions(FileSystem fs, JavaSparkContext sc, Options options) {

    }
}
