package com.mobin.common;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.Serializable;


/**
 * Created by Mobin on 2017/9/29.
 */
public class DataFile implements Serializable{
    public final String name;
    public final boolean isLzoFile;
    private boolean snappyCodec;
    public boolean deleteSrcFilesOnPartitioned;

    public DataFile(String name){
        this.name = name;
        this.isLzoFile = isLzoFile(name);
    }

    public DataFile(Path file, boolean isLzoFile){
        this.name = file.toString();
        this.isLzoFile = isLzoFile;
    }

    public static boolean isLzoFile(String file){
        return file.toLowerCase().endsWith(".lzo");
    }

    public static boolean isLzoFile(Path file){
        return isLzoFile(file.getName());
    }

    public long getModificationTime(FileSystem fs) throws IOException {
        if(fs.exists(new org.apache.hadoop.fs.Path(name)))
                return fs.getFileStatus(new Path(name)).getModificationTime();
        else if(fs.exists(new Path(name + DataPartitioner.PARTITIONED)))
            return fs.getFileStatus(new Path(name + DataPartitioner.PARTITIONED)).getModificationTime();
        return 0;
    }

    public void delete(FileSystem fs) throws IOException {
        if (fs.exists(new Path(name + DataPartitioner.PARTITIONED)))
            fs.delete(new Path(name + DataPartitioner.PARTITIONED),false);
    }

    @Override
    public String toString(){
        return  "DataFile[" + name + "]";
    }
}
