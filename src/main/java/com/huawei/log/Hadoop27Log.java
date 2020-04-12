package com.huawei.log;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.logaggregation.LogCLIHelpers;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;

public class Hadoop27Log {
    private static Configuration yarnConfiguration;
    private static LogCLIHelpers logCLIHelpers;
    private static final Logger LOGGER = LoggerFactory.getLogger(Hadoop27Log.class);

    static {
        yarnConfiguration = new YarnConfiguration();

        yarnConfiguration.addResource("E:\\openSource\\hadoop_learning\\src\\main\\resources\\core-site.xml");
        yarnConfiguration.addResource("E:\\openSource\\hadoop_learning\\src\\main\\resources\\hdfs-site.xml");
        yarnConfiguration.addResource("E:\\openSource\\hadoop_learning\\src\\main\\resources\\yarn-site.xml");

        /*yarnConfiguration.set(YarnConfiguration.RM_ADDRESS, "192.168.2.177:8032");
        yarnConfiguration.set(YarnConfiguration.RM_HOSTNAME, "192.168.2.177");
        yarnConfiguration.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, "192.168.2.177:8030");
        yarnConfiguration.set(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS, "192.168.2.177:8031");
        yarnConfiguration.set(YarnConfiguration.RM_WEBAPP_ADDRESS, "192.168.2.177:8088");
        yarnConfiguration.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);*/

        /*YarnClient yarnClient = new YarnClientImpl();
        yarnClient.init(yarnConfiguration);
        yarnClient.start();*/
        logCLIHelpers = new LogCLIHelpers();
        logCLIHelpers.setConf(yarnConfiguration);
    }

    public static Configuration getYarnConfiguration() {
        return yarnConfiguration;
    }

    public static int dumpAContainersLogs(String appId, String containerId, String nodeId, String jobOwner, PrintStream out) throws IOException {
        Path remoteRootLogDir = new Path(getYarnConfiguration().get("yarn.nodemanager.remote-app-log-dir", "/tmp/logs"));

        String suffix = LogAggregationUtils.getRemoteNodeLogDirSuffix(getYarnConfiguration());
        Path remoteAppLogDir = LogAggregationUtils.getRemoteAppLogDir(remoteRootLogDir, ConverterUtils.toApplicationId(appId), jobOwner, suffix);

        RemoteIterator nodeFiles;
        try {
            Path qualifiedLogDir = FileContext.getFileContext(getYarnConfiguration()).makeQualified(remoteAppLogDir);
            nodeFiles = FileContext.getFileContext(qualifiedLogDir.toUri(), getYarnConfiguration()).listStatus(remoteAppLogDir);
        } catch (FileNotFoundException var16) {
            logDirNotExist(remoteAppLogDir.toString());
            return -1;
        }

        boolean foundContainerLogs = false;

        while(nodeFiles.hasNext()) {
            FileStatus thisNodeFile = (FileStatus)nodeFiles.next();
            String fileName = thisNodeFile.getPath().getName();
            if (fileName.contains(LogAggregationUtils.getNodeString(nodeId)) && !fileName.endsWith(".tmp")) {
                AggregatedLogFormat.LogReader reader = null;

                try {
                    reader = new AggregatedLogFormat.LogReader( getYarnConfiguration(), thisNodeFile.getPath());
                    if (dumpAContainerLogs(containerId, reader, out, thisNodeFile.getModificationTime()) > -1) {
                        foundContainerLogs = true;
                    }
                } finally {
                    if (reader != null) {
                        reader.close();
                    }

                }
            }
        }

        if (!foundContainerLogs) {
            containerLogNotFound(containerId);
            return -1;
        } else {
            return 0;
        }
    }

    private static void logDirNotExist(String remoteAppLogDir) {
        System.out.println(remoteAppLogDir + " does not exist.");
        System.out.println("Log aggregation has not completed or is not enabled.");
    }

    private static void containerLogNotFound(String containerId) {
        System.out.println("Logs for container " + containerId + " are not present in this log-file.");
    }

    public static int dumpAContainerLogs(String containerIdStr, AggregatedLogFormat.LogReader reader, PrintStream out, long logUploadedTime) throws IOException {
        AggregatedLogFormat.LogKey key = new AggregatedLogFormat.LogKey();

        DataInputStream valueStream;
        for(valueStream = reader.next(key); valueStream != null && !key.toString().equals(containerIdStr); valueStream = reader.next(key)) {
            key = new AggregatedLogFormat.LogKey();
        }

        if (valueStream == null) {
            return -1;
        } else {
            boolean foundContainerLogs = false;

            while(true) {
                try {
                    AggregatedLogFormat.LogReader.readAContainerLogsForALogType(valueStream, out, logUploadedTime);
                    foundContainerLogs = true;
                } catch (EOFException var10) {
                    if (foundContainerLogs) {
                        return 0;
                    }

                    return -1;
                }
            }
        }
    }

}
