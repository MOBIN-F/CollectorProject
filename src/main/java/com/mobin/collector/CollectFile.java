package com.mobin.collector;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tachyon.client.OutStream;

import java.io.*;
import java.util.ArrayList;


/**
 * Created by Mobin on 2017/6/17.
 */
public class CollectFile {

    private static final Logger log = LoggerFactory.getLogger(CollectFile.class);

    private final FileSystem fs;
    File file;
    String targetFile;
    private boolean copied;
    private long fileId;
    
    public CollectFile(FileSystem fs, File file, String targetFile){
        this.fs = fs;
        this.file = file;
        this.targetFile = targetFile;
    }

    public CollectFile(FileSystem fs, File file, String targetFile,long fileId){
        this.fs = fs;
        this.file = file;
        this.targetFile = targetFile;
        this.fileId = fileId;
    }


    public String getName() {
        return file.getName();
    }
    
    public long getModificationTime() {
        return file.lastModified();
    }
    
    public boolean isCopied() {
        return copied;
    }
    
    public void copy() throws IOException {
        System.out.println(99);
        InputStream in = null;
        OutputStream out = null;
        String fileName = file.getName();
        if (fileName.endsWith(Collector.DONE)) {
            fileName = fileName.substring(0, fileName.length() - Collector.DONE.length());
        }else if (fileName.endsWith(Collector.DOWN)) {
            fileName = fileName.substring(0, fileName.length() - Collector.DOWN.length());
        }
        
        long startTime = System.currentTimeMillis();
        boolean created = false;
        try {
            in = new FileInputStream(file);
            out = fs.create(new Path(targetFile));
            created = true;
            //最后个参数为是否同时关闭输入流和输出流
            IOUtils.copyBytes(in, out, 4096, false);
            long endTime = System.currentTimeMillis();
            copied = true;
            log.info("file copied, collectingFile:" + file + ", size: " + file.length()/ 1024 + "k , targetFile:" 
                    + targetFile + ", time :"+  (endTime - startTime) + "ms" );
        } catch (Exception e) {
           if (created){  // 在copy的过程中发生异常，但是文件已经创建，此时需要删除
               try{
                   IOUtils.closeStream(out);
                   out = null;
                   fs.delete(new Path(targetFile),true);
               }catch (Exception e2){
                   log.error("Failed to delete targetFile: " + targetFile, e2);
               }
           }
            log.error("Failed to copy file:" + file, e);
        }finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }
    }

}
