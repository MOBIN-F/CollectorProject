package com.mobin.collector;

import com.mobin.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Created by Mobin on 2017/5/17.
 */
public class MobinTest {
    @Test
    public void mobintest() throws IOException {
        System.out.println(System.getProperty("mm"));
        System.out.println(MobinTest.class.getResourceAsStream("/File_conf.properties"));
        System.out.println(Config.getStringProperty("Mobin_prefix"));

        File file = new File("E:\\collectProjectFile\\TEST.txt.down");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.newInstance(conf);
        CollectorOptions collectorOptions = new CollectorOptions();
        collectorOptions.dateTime = "20170628";
        CollectFile collectFile = new CollectFile(fs, file, "F:\\test\\mobin");
        collectFile.copy();

    }

    @Test
    public void test(){
        System.out.println(System.getProperty("java.io.tmpdir"));
    }
}
