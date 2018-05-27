package com.mobin.collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Mobin on 2017/6/12.
 */
public class Testm {
    public static void main(String[] args) throws InterruptedException {
           String dateTime = "2017122711";
           String str = "-dateTime " + dateTime + " "
                   + "-collectors mobin "
                   + "-parallelizable true ";

           DataCollector.main(str.split(" "));

//         Logger log = LoggerFactory.getLogger(Testm.class);
//        log.info("555");
    }
}
