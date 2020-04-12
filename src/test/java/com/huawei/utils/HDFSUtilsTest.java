package com.huawei.utils;

import org.junit.Test;

import java.io.IOException;

/**
 * @Auther: liufj
 * @Date: 2019/7/27 09:02
 * @Description:
 */
public class HDFSUtilsTest {
    @Test
    public void testLs() throws IOException {
        HDFSUtils.ls("/");
    }

    @Test
    public void testCat () throws IOException {
        HDFSUtils.cat("/user/input/word.txt");
    }
    @Test
    public void testGet() throws IOException {
        HDFSUtils.get("/user/input/word.txt", "D:\\");
    }
}
