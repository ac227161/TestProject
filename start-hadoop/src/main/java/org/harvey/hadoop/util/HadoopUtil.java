package org.harvey.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HadoopUtil {
    private static final transient ThreadLocal<Configuration> hadoopLocalConfig = new ThreadLocal<>();

    public static Configuration getLocalConfiguration() {
        Configuration conf = hadoopLocalConfig.get();
        if (conf == null) {
            conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://master:8020");
            conf.set("mapreduce.app-submission.cross-platform", "true");
            conf.set("mapreduce.framework.name", "yarn");
            conf.set("mapreduce.job.jar", "D:\\Software\\Java\\project\\start-hadoop\\target\\start-hadoop-1.0-jar-with-dependencies.jar");
            //hbase
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("hbase.zookeeper.quorum","192.168.0.115,192.168.0.114,192.168.0.113");
            hadoopLocalConfig.set(conf);
        }
        return conf;
    }

    public static void deletePath(Configuration conf, Path path) throws IOException {
        FileSystem fs = FileSystem.get(path.toUri(), conf);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
    }

    public static FileSystem getFileSystem(String path) throws IOException {
        return getFileSystem(new Path(makeURI(path)));
    }

    public static FileSystem getFileSystem(Path path) throws IOException {
        Configuration conf = getLocalConfiguration();
        return getFileSystem(path, conf);
    }

    public static FileSystem getFileSystem(Path path, Configuration conf) {
        try {
            return path.getFileSystem(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static URI makeURI(String filePath) {
        try {
            return new URI(fixWindowsPath(filePath));
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Cannot create FileSystem from URI: " + filePath, e);
        }
    }

    public static String fixWindowsPath(String path) {
        // fix windows path
        if (path.startsWith("file://") && !path.startsWith("file:///") && path.contains(":\\")) {
            path = path.replace("file://", "file:///");
        }
        if (path.startsWith("file:///")) {
            path = path.replace('\\', '/');
        }
        return path;
    }
}
