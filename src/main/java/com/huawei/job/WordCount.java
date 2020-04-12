/*
package com.huawei.job;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

*/
/**
 * @author: liufj
 * @Date: 2019/8/25 10:13
 * @Description:
 *//*

@Slf4j
public class WordCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable INT_WRITABLE = new IntWritable(1);

        private Text word = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
            while (stringTokenizer.hasMoreTokens()) {
                word.set(stringTokenizer.nextToken());
                context.write(word, INT_WRITABLE);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();		// 分组累加
            }
            result.set(sum);
            context.write(key, result);	// 按相同的key输出
        }
    }

    public static void main(String[] args) {
        try {
            if (args.length < 1) {
                log.info("args length is 0");
                run("test");
            } else {
                run(args[0]);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void run(String name) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");				// 创建一个任务提交对象
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);	// 指定Map计算的类
        job.setCombinerClass(IntSumReducer.class);	// 合并的类
        job.setReducerClass(IntSumReducer.class);	// Reduce的类
        job.setOutputKeyClass(Text.class);			// 输出Key类型
        job.setOutputValueClass(IntWritable.class);	// 输出值类型

        // 设置统计文件在分布式文件系统中的路径
        String inPath = "hdfs://192.168.88.111:9000/user/input/word.txt";
        // 设置输出结果在分布式文件系统中的路径
        String outPath = "hdfs://192.168.88.111:9000/user/output";

        FileInputFormat.addInputPath(job, new Path(inPath));		// 指定输入路径
        FileOutputFormat.setOutputPath(job, new Path(outPath));	// 指定输出路径

        int status = job.waitForCompletion(true) ? 0 : 1;

        System.exit(status);										// 执行完MR任务后退出应用
    }


}
*/
