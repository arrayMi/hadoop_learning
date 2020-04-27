package com.huawei.utils;

import com.huawei.log.HadoopLogUtils;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.logaggregation.LogCLIHelpers;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestHadoopLogUtils {

    private LogCLIHelpers logCLIHelpers;

    @Before
    public void initLogCLIHelpers(){
        this.logCLIHelpers = new LogCLIHelpers();
        logCLIHelpers.setConf(HadoopLogUtils.getYarnConfiguration());
    }

    @Test
    public void testGetConfig() throws IOException {
        //logCLIHelpers.dumpAllContainersLogs(ApplicationIdPBImpl.newInstance(1586654872264l,1), "root", System.out);
    }

    @Test
    public void testAppId(){
        System.out.println(ApplicationIdPBImpl.newInstance(1586608651701l,1));
    }

    @Test
    public void testDump4Para() throws IOException {
        logCLIHelpers.dumpAContainersLogs("application_1586654872264_0001", "container_1586654872264_0001_01_000001", "master:42139", "root");
    }

    @Test
    public void testConsumerPrint() throws IOException {
        OutputStream in = new FileOutputStream(new File("E:\\openSource\\hadoop_learning\\target\\a.txt"));
        PrintStream printStream = new PrintStream(in);
        List<String> logType = new ArrayList<>(1);
        logType.add("stderr");
        HadoopLogUtils.dumpAContainersLogs("application_1587284642166_0001",
                "container_1587284642166_0001_01_000001",
                "master:42757",
                "root", System.out ,logType);
    }

    @Test
    public void testConvertAppId() {
        System.out.println(ConverterUtils.toApplicationId("application_1586866201216_0001").toString());
    }

    @Test
    public void testGetContaines() throws IOException {
        Map<String, String> map = HadoopLogUtils.getContaines("application_1587284642166_0001", "root");
        map.forEach((k, v) ->{
            System.out.println("containeId is: " + k + "nodeId is: " + v);
        });
    }

    @Test
    public void testConvert() {
        System.out.println(HadoopLogUtils.convert("application_1586866201216_0001").toString());
    }
}
