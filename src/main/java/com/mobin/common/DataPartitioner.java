package com.mobin.common;

import com.mobin.collector.FSUtils;
import com.mobin.config.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.BufferedIterator;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Mobin on 2017/9/29.
 */
public class DataPartitioner implements Serializable, Partitioner{
    private static final Logger log = LoggerFactory.getLogger(DataPartitioner.class);
    public static final String PARTITIONED = ".partitioned";
    @Override
    public void createOrAppendPartitions(FileSystem fs, JavaSparkContext sc, Options options) {

    }

    protected void createdOrAppendPartition(FileSystem fs, JavaSparkContext sc,
                                            Options options, String dataPath, String partitionDir) throws Exception {
        if (dataPath == null)
            return;;
        if (!FSUtils.exists(fs, dataPath)){
            log.warn("数据文件路径： " + dataPath + "不存在");
            return;
        }

        ArrayList<DataFile> newFiles = getNewFiles(fs,dataPath,partitionDir);
    }

    /**
     *
     * @param fs
     * @param path：SRC路径
     * @param partitionDir：ETL路径
     * @return
     * @throws IOException
     */
    private static ArrayList<DataFile> getNewFiles(FileSystem fs,String path,String partitionDir) throws Exception {
        ArrayList<DataFile> newFiles = new ArrayList<>();
        ArrayList<DataFile> allFiles = getAllFiles(fs, path);  //获取指定日期SRC下的所有文件
        if (FSUtils.exists(fs, partitionDir)){
            fs.mkdirs(new Path(partitionDir));
            newFiles = allFiles;
        }else {
            String partitionMetaFataFile = getPartitionMetaDataFile(partitionDir);
            if (!FSUtils.exists(fs, partitionMetaFataFile)){
                newFiles = allFiles;
            }else {
                HashMap<String, Long> map = readPartitionMetaDataFile(fs, partitionDir);
                for (DataFile dataFile: allFiles){
                    Long lastModificationTime = map.get(dataFile.name);
                    if (lastModificationTime == null){
                        newFiles.add(dataFile);
                    }else {
                        long modificationTime = dataFile.getModificationTime(fs);
                        if (modificationTime > lastModificationTime)
                            newFiles.add(dataFile);
                    }
                }
            }
        }
        return newFiles;
    }

    private static HashMap<String, Long> readPartitionMetaDataFile(FileSystem fs,String partitionMetaDataFile) throws Exception {
        HashMap<String, Long> map = new HashMap<>();
        try(FSUtils.BufferedReaderIterable bri = FSUtils.createBufferedReadIterable(fs,partitionMetaDataFile)){
            for (String line : bri){
                int pos = line.indexOf(",");
                String fileName = line.substring(0, pos);
                long lastModificationTime = Long.parseLong(line.substring(pos + 1));
                map.put(fileName, lastModificationTime);
            }
        }
        return map;
    }

    private static String getPartitionMetaDataFile(String partitionDir){
        return partitionDir + "/_PARTITION_/metaData.txt";
    }

    private static ArrayList<DataFile> getAllFiles(FileSystem fs,String path) throws IOException {
        ArrayList<DataFile> files = new ArrayList<>();
        if (FSUtils.isFile(fs, path)) {
            files.add(new DataFile(path));
            return files;
        }
        FSUtils.getDataFileRecurslvely(fs,path,files);
        return files;
    }
}
