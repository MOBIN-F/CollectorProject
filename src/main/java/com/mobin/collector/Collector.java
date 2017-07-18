package com.mobin.collector;

import com.mobin.common.SmallLRUCache;
import com.mobin.collector.FSUtils.*;
import com.mobin.config.Config;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.tools.cmd.gen.AnyVals;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Filter;

/**
 * Created by Mobin on 2017/5/7.
 * 采集类
 */
public abstract  class Collector implements  Runnable {
    static final Logger log = LoggerFactory.getLogger(Collector.class);
    static final String DONE = ".done";
    static final String DOWN = ".down";
    static final Charset CHARSET =  Charset.forName("GBK");

    FileSystem fs;
    String collectorPath;
    String targetPath;
    String type;
    CollectorOptions options;

    public static final String NEW_FILES = "_NEW_FILES_";
    public static final String _COPIED_FILES_ = "_COPIED_FILES_";

    public abstract Map<String, ArrayList<CollectFile>> getNewFiles();

    public abstract String getFileDateTime(String fileName);

    static final FilenameFilter downFilter = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith("DOWN");
        }
    };

    private Thread shutdownHook;
    private volatile boolean shutdown;
    @Override
    public void run() {
          shutdownHook = new Thread(){
              @Override
              public void run() {
                  //如果JVM被强制关闭，首先会执行该run方法
                  Collector.this.shutdown = true;
                  awaitFinsh();
              }
          };
        //注册钩子，保证程序能正常执行完
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        try {
            copyFile();
        }finally {
            notifyFinish();
        }
    }

    private synchronized void awaitFinsh(){
        log.info("awaiting copy action" + this + "finish");
        try {
            wait();
            log.info(this + " finish");
        } catch (InterruptedException e) {
            log.error("await " + this + " finish exception", e);
        }
    }

    private void copyFile(){
        Map<String, ArrayList<CollectFile>> dateTimeToNewFilesMap = getNewFiles();
        try {
            if (options.parallelizable) {
                copyFilesParallel(dateTimeToNewFilesMap);
            }
            copyFileSerially(dateTimeToNewFilesMap);
        } catch (IOException e) {
            log.error("Failed to copy files", e);
        }
    }

    private void copyFilesParallel(Map<String, ArrayList<CollectFile>> dateTimeToNewFilesMap){
        ThreadPoolExecutor threadPoolExecutor = FSUtils.getThreadPoolExecutor(Runtime.getRuntime().availableProcessors() *2);
         int count = 0;
        for (Map.Entry<String, ArrayList<CollectFile>> entry: dateTimeToNewFilesMap.entrySet()) {
            String dateTime = entry.getKey();
            ArrayList<CollectFile> collectFiles = entry.getValue();

            ArrayList<Future<?>> futures = new ArrayList<>();
            final ArrayList<CollectFile> copiedFiles = new ArrayList<>();
            final AtomicLong size = new AtomicLong();
            log.info("Collector: " + this + ", dateTime:" + dateTime + ", collectingFiles size: "
                                  + collectFiles.size() + ", threadPoolExecutor szie:" + threadPoolExecutor.getMaximumPoolSize());
            for (final CollectFile collectFile: collectFiles){
                Runnable task = new Runnable() {
                    @Override
                    public void run() {
                        try {
                            collectFile.copy();
                            if (collectFile.isCopied()){
                                synchronized (copiedFiles){  //ArrayList不是同步的
                                    copiedFiles.add(collectFile);
                                    size.addAndGet(collectFile.file.length());
                                }
                            }
                        } catch (IOException e) {
                            log.error("Failed to copy " + collectFile.file, e);
                        }
                    }
                };
            }

        }
    }

    private void copyFileSerially(Map<String, ArrayList<CollectFile>> dateTimeToNewFilesMap) throws IOException {
        for (Map.Entry<String,ArrayList<CollectFile>> entry: dateTimeToNewFilesMap.entrySet()) {
            String dateTime = entry.getKey();
            ArrayList<CollectFile> collectingFiles = entry.getValue();   //需要采集的文件
            ArrayList<CollectFile> copiedFiles = new ArrayList<>();
            long size = 0;
            for (CollectFile collectingFile: collectingFiles) {
                collectingFile.copy();
                if(collectingFile.isCopied()) {   //已经copied文件
                    copiedFiles.add(collectingFile);
                    size += collectingFile.file.length();
                }
                if (size > 0) {
                    finish(dateTime, copiedFiles);
                }
            }
        }
    }

    private void finish(String dateTime, ArrayList<CollectFile> copiedFiles) {
        if (copiedFiles.isEmpty()) {
            return;
        }

        String id = FSUtils.getUID() + ".txt";
        OutputStream out = null;
        String newFile = targetPath + NEW_FILES + File.separator + "f_" + dateTime + "_" + id;
        try {
            out = new BufferedOutputStream(fs.create(new Path(newFile)));
            for (CollectFile cf : copiedFiles) {
                if (cf.isCopied()){
                    out.write((cf.file + "\n").getBytes(CHARSET));
                }
            }
            out.flush();
        } catch (IOException e) {
            log.error("Failed to write file: " + newFile, e);
        }finally {
            IOUtils.closeStream(out);
        }
        updateCopiedDataFiles(dateTime, copiedFiles);
    }

    private void updateCopiedDataFiles(String dateTime, ArrayList<CollectFile> newFiles) {
      String copiedFileName = getCopiedFileName(dateTime);  //获取已copied文件路径   srcPath + _COPIED_FILES_ + "/" + date + "/" + dateTime + ".txt"
      Path path = new Path(copiedFileName);  //获取已copied文件
        try {
            OutputStream os = FSUtils.openOutputStream(fs, path);
            try {
                StringBuilder buff = new StringBuilder();
                for (CollectFile cf : newFiles) {
                    if (cf.isCopied()) {
                        long modification = cf.getModificationTime();
                        buff.append(cf.file.getAbsolutePath()).append(",");
                        buff.append(modification).append(",").append(cf.file.length()).append("\n");
                    }
                }
                os.write(buff.toString().getBytes("UTF-8"));
                os.flush();
            }finally {
                FSUtils.closeStreamSilently(os);
            }
        } catch (IOException e) {
            log.error("Failed to updateCopiedDataFiles: " + copiedFileName);
        }
    }

    private synchronized void notifyFinish() {
        if (!shutdown) {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        }
        notify();
    }

    protected Map<String, ArrayList<CollectFile>> getNewFiles(String dir, FilenameFilter filter){
        ArrayList<String> list = new ArrayList<>(1);
        list.add(dir);
        return getNewFiles(list, filter);
    }

    public List<String> getDateDir(){
        if (collectorPath == null || collectorPath.isEmpty()) {
            return new ArrayList<>(0);
        }
        String[] dirs = collectorPath.split(",");
        return getDateDir(dirs, Config.dateFormat);
    }

    public List<String> getDateDirs(List<String> dirs) {
        return getDateDir(dirs.toArray(new String[0]), Config.dateFormat);
    }

    /**
     *
     * @param dirs:collectorPath.split(",")
     * @param dateFormat:时间格式
     * @return  需要采集的目录，比如[xx/zz/20170628,xx/zzz/20170628]
     */
    public List<String> getDateDir(String[] dirs, SimpleDateFormat dateFormat){
        for (int i = 0, length = dirs.length; i < length; i ++) {
            dirs[i] = FSUtils.appendSlash(dirs[i]);
        }
        List<String> dateDirs;
        //只采集某个小时的数据
        if (options.dateTime != null) {
             dateDirs = new ArrayList<>(dirs.length);
            String date = FSUtils.getDate(options.dateTime);  //date:20170624 dateTime:2017032410
            for (int i = 0, length = dirs.length; i < length; i ++) {
                     dateDirs.add(dirs[i] + date + File.separator);   //配置中的采集目录是xx/xxx,到这里拼接日期变成xx/xxx/20170624/
            }
        } else {  //集采连续几天的数据
            String startTime = FSUtils.getDate(options.startTime);
            String currentDate = FSUtils.getCurrentDate(dateFormat);
            List<String> dates;
            try {
                dates = FSUtils.getDates(startTime, currentDate, dateFormat);
            } catch (ParseException e) {
                log.error("Invalid date" , e);
                return new ArrayList<>(0);
            }
            dateDirs = new ArrayList<>(dirs.length * dates.size());

            for (String date : dates) {
                for (int i = 0, length = dirs.length; i < length; i ++) {
                    dateDirs.add(dirs[i] + date + File.separator);
                }
            }
        }
     return dateDirs;
    }



    protected Map<String, ArrayList<CollectFile>> getNewFiles(List<String> dirs, FilenameFilter filter){
        dirs = getModifiedDirs(dirs);    //需要copy的文件队列
        log.info("modified dirs:" + dirs);
        Map<String, ArrayList<CollectFile>> dateTimeToNewFilesMap = new TreeMap<>();
        Map<String, HashSet<String>> dateTimeToCopiedFilesMap = new TreeMap<>();
        Map<String, AtomicLong> dateTimeToFileIdMap = new TreeMap<>();

        for (String d : dirs) {
            File dir = new File(d);
            File[] files;
            if (filter != null){
                files = dir.listFiles(filter);
            } else {
                files = dir.listFiles();
            }
            if (files == null || files.length == 0) {
                continue;
            }

            log.info("dir: " + dir + ", files:" + files.length);
            for (File f : files) {
                if (!isCopyableFile(f)) {  //延迟判断文件是否已经写完，如果目录时间戳在延迟间隔不改了，必需删除目录缓存
                    removeDirCache(d);
                } else {
                    String name = f.getName();
                    String dateTime;
                    try {
                        dateTime = getFileDateTime(name);
                    }catch (Exception e){
                        log.warn("文件名不包含日期时间，可能是一个无效的文件或文件名");
                        continue;
                    }
                    if (options.dateTime != null) {   //过滤其他小时的文件
                        if (!options.dateTime.equals(dateTime)) {
                            continue;
                        }
                    } else if (options.startTime != null) {
                        if (!options.startTime.equals(dateTime)) {
                            continue;
                        }
                    }
                    //文件已经入库过
                    if (isCopied(dateTime, f , dateTimeToCopiedFilesMap, dateTimeToFileIdMap)) {
                        continue;
                    }
                    //新文件
                    String date = dateTime.substring(0, dateTime.length() -2);
                    String targetDir = targetPath + date + File.separator + dateTime + File.separator;
                    ArrayList<CollectFile> newFiles = dateTimeToNewFilesMap.get(dateTime);
                    if (newFiles == null) {
                        newFiles = new ArrayList<>();
                        dateTimeToNewFilesMap.put(dateTime, newFiles);
                    }
                    newFiles.add(new CollectFile(fs, f, targetDir, -1));
                }
            }
        }
        return  dateTimeToNewFilesMap;
    }

    private boolean isCopied(String dateTime, File f, Map<String, HashSet<String>> dateTimeToCopiedFilesMap, Map<String, AtomicLong> dateTimeToFileIdMap) {
        HashSet<String> copiedFiles = dateTimeToCopiedFilesMap.get(dateTime);
        if (copiedFiles == null) {  //新文件
            try {
                copiedFiles = readCopiedFiles(dateTime, dateTimeToFileIdMap);
                dateTimeToCopiedFilesMap.put(dateTime, copiedFiles);
            } catch (IOException e) {
                log.error("Failed to readCopiedFiles, dateTime: " + dateTime, e);
                return false;
            }
        }
        return copiedFiles.contains(f.getAbsolutePath());
    }

    private HashSet<String> readCopiedFiles(String dateTime, Map<String, AtomicLong> dateTimeToFileIdMap) throws IOException {
        String copiedFileName = getCopiedFileName(dateTime);
        HashSet<String> copiedFiles = new HashSet<>();
        if (!fs.exists(new Path(copiedFileName))) {  //判断该路径下是否存在该文件，如果不存在
            dateTimeToFileIdMap.put(dateTime, new AtomicLong(0));
            return copiedFiles;
        }
        //如果存在,就读取文件中的内容
        try(BufferedReaderIterable bri = FSUtils.createBufferedReadIterable(fs, copiedFileName)){
            for (String line : bri) {
                if (line.isEmpty()) {
                    continue;
                }
                String[] a = line.split(",", -1);
                copiedFiles.add(a[0]);
                bri.incrementVaildRecords();
            }
            dateTimeToFileIdMap.put(dateTime, new AtomicLong(bri.getVaildRecords()));
        }
      return copiedFiles;
    }

    private String getCopiedFileName(String dateTime) {
        String date = dateTime.substring(0, dateTime.length() - 2);
        //srcPath + _COPIED_FILES_ + "/" + date + "/" + dateTime + ".txt"文件保存的是文件的路径
        return targetPath + _COPIED_FILES_ + File.separator+ date + File.separator + dateTime + ".txt";
    }


    private boolean isCopyableFile(File f) {
        long lastModifiedTime = f.lastModified();
        if (f.isFile() && f.length() >0 && lastModifiedTime + options.checkInterval < System.currentTimeMillis()) {  //说明在这时间间隔内文件没有被修改
            return true;
        }
        return false;
    }

    //非线程安全，需要同步
    private static final SmallLRUCache<String, Long> dirCache = new SmallLRUCache<>(500);

    private void removeDirCache(String dir) {
        synchronized (dirCache) {
            dirCache.remove(dirCache.get(dir));
        }
    }

    private List<String> getModifiedDirs(List<String> dirs) {
        List<String> modifiedeDirs = new ArrayList<>();
        for (String dir: dirs) {
            File d = new File(dir);
            if (!d.exists()) {
                log.warn("Dir not exists : " + dir);
                continue;
            }

            synchronized (dirCache) {
                Long cacheLastModified  = dirCache.get(dir); //获取文件的时间戳（缓存值）
                long lastModified = d.lastModified();  // （实时值）
                if (cacheLastModified == null || lastModified > cacheLastModified) {  //说明有新文件或文件有更新
                    modifiedeDirs.add(dir);
                    dirCache.put(dir, lastModified);
                }
            }
        }
        return modifiedeDirs;
    }
}
