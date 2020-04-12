package com.huawei.utils;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @Auther: liufj
 * @Date: 2019/7/26 19:20
 * @Description:
 */
public class HDFSUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSUtils.class);
    private static Configuration configuration = null;
    private static FileSystem fileSystem = null;

    static {
        configuration = new Configuration();
        // 指定hdfs的nameservice为cluster1,是NameNode的URI
        configuration.set("fs.defaultFS", "hdfs://192.168.88.111:9000");
        //configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("hadoop.home.dir", "E:\\DevSoft\\hadoop-3.2.0");
        configuration.set("dfs.permissions.enabled", String.valueOf(false));
        try {
            fileSystem = FileSystem.get(configuration);
        } catch (IOException e) {
            LOGGER.error("初始化文件系统失败!!!!");
            e.printStackTrace();
        }
    }

    /**
     * 获取文件列表
     * @param remotePath
     * @throws IOException
     */
    public static void ls(String remotePath) throws IOException {
        Path path = new Path(remotePath);
        FileStatus [] fileStatuses = fileSystem.listStatus(path);
        Path [] listPaths = FileUtil.stat2Paths(fileStatuses);
        for (Path listPath : listPaths) {
            System.out.println(listPath);
        }
    }

    /**
     * 读取文件
     * @param remotePath
     * @throws IOException
     */
    public static void cat (String remotePath) throws IOException {
        Path path = new Path(remotePath);
        if(fileSystem.exists(path)) {
            FSDataInputStream inputStream = fileSystem.open(path);
            FileStatus fileStatus = fileSystem.getFileStatus(path);
            byte[] bytes = new byte[1024];
            int len = -1;
            ByteArrayOutputStream stream = new ByteArrayOutputStream();

            while ((len = inputStream.read(bytes)) != -1) {
                stream.write(bytes, 0, len);
            }
            inputStream.close();
            stream.close();
            System.out.println(new String(stream.toByteArray()));
        }
    }

    /**
     * 下载hdfs文件
     * @param remotePath
     */
    public static void get (String remotePath, String localPath) throws IOException {
        Path src = new Path(remotePath);
        Path dst = new Path(localPath);
        fileSystem.copyToLocalFile(src, dst);
        LOGGER.info("下载完成----");
    }

}
