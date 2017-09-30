package com.mobin.common;


import com.mobin.config.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by Mobin on 2017/9/29.
 */
public interface Partitioner {
    void createOrAppendPartitions(FileSystem fs, JavaSparkContext sc, Options options);
}
