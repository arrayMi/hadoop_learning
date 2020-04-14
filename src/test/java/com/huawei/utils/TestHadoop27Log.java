package com.huawei.utils;

import com.huawei.log.Hadoop27Log;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.logaggregation.LogCLIHelpers;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

public class TestHadoop27Log {

    private LogCLIHelpers logCLIHelpers;

    @Before
    public void initLogCLIHelpers(){
        this.logCLIHelpers = new LogCLIHelpers();
        logCLIHelpers.setConf(Hadoop27Log.getYarnConfiguration());
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
        Hadoop27Log.dumpAContainersLogs("application_1586654872264_0001",
                "container_1586654872264_0001_01_000001",
                "master:42139",
                "root", printStream);
    }

    @Test
    public void testConvertAppId() {
        System.out.println(ConverterUtils.toApplicationId("application_1586866201216_0001").toString());
    }
}
