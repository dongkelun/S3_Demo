package com.dkl.s3.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

public class Flink_S3_Demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.readTextFile("s3://txt/test.txt").print();
        env.execute("Read S3");

        env.fromElements("abc", "def").writeAsText("s3://test-s3-write/test_flink_program", OVERWRITE);
        env.execute("Write S3");

        env.readTextFile("s3://test-s3-write/test_flink_program").print();
        env.execute("Read S3");
    }
}
