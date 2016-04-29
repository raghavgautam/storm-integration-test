package com.hortonworks.storm.st;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by temp on 4/28/16.
 */
public class TimeUtil {
    private static Logger log = LoggerFactory.getLogger(TimeUtil.class);

    public static void sleepSec(int sec) {
        try {
            Thread.sleep(sec * 1000);
        } catch (InterruptedException e) {
            log.warn("Caught exception: " + ExceptionUtils.getFullStackTrace(e));
        }
    }
}
