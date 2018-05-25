package org.harvey.hadoop.sqoop;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
//import org.apache.sqoop.Sqoop;
//import org.apache.sqoop.tool.ImportTool;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqoopUtil {
    /*
    public static int importHbase(String sqoopScript) {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://master:8020"); //设置HDFS服务地址

        Pattern pattern = Pattern.compile(".*(--query)\\s+\"(.*?)\".*", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sqoopScript);
        List<String> list = Lists.newArrayList();
        if (matcher.matches()) {
            String p1 = matcher.group(1);
            String p2 = matcher.group(2);
            list.add(p1);
            list.add(p2);
            sqoopScript = sqoopScript.replace(p1, "").replace("\"" + p2 + "\"", "");
        }
        String[] param = sqoopScript.split("\\s+");
        for (String s : param) {
            list.add(s);
        }
        String[] arg = list.toArray(new String[list.size()]);
        ImportTool importer = new ImportTool();
        Sqoop sqoop = new Sqoop(importer);
        sqoop.setConf(conf);
        return Sqoop.runSqoop(sqoop, arg);
    }*/

    public static void main(String[] args) throws Exception {
        //String sqoopScript = "--connect jdbc:oracle:thin:@202.202.3.3:1521:orcl --username mzxt --password mzxt --query \"select (ZKLB)as ROWKEY,ZKLB,LBMC,MBMC3,MBWJ3,SYZK,ZTBZ,MBWJ,MBMC2,SSZK,MBWJ1,MBMC1,MBMC,MBWJ2 from MZ_ZKBL where \\$CONDITIONS\" --hbase-table mzxt:MZ_ZKBL --hbase-row-key ROWKEY --column-family f1 -m 1";
//        String sqoopScript = "--connect jdbc:oracle:thin:@192.168.0.114:1521:orcl --username ogg --password ogg --query \"SELECT  ID||'|'||ID2 AS ROWKEY,ID,ID2,VALUE1 FROM TEST3 where $CONDITIONS\" --hbase-table BDTEST:TEST3 --hbase-row-key ROWKEY --column-family cf -m 1";
//        System.out.println(importHbase(sqoopScript));
//        System.out.println("test");

        String str = "18/04/02 18:05:38 INFO mapreduce.ImportJobBase: Retrieved 2 records.";
        Pattern pattern = Pattern.compile(".*Retrieved\\s+(\\d+?)\\s+records.*", Pattern.CASE_INSENSITIVE);
        Matcher m = pattern.matcher(str);
        if(m.matches()) {
            System.out.println(m.group(1));
        }

        String sqoopScript = "sqoop import --connect jdbc:oracle:thin:@192.168.0.114:1521:orcl --username ogg --password ogg --query \"SELECT  ID||'|'||ID2 AS ROWKEY,ID,ID2,VALUE1 FROM TEST3 where \\$CONDITIONS\" --hbase-table BDTEST:TEST3 --hbase-row-key ROWKEY --column-family cf -m 1";
        String hostname = "192.168.0.111";
        int port = 22;
        String username = "root";
        String password = "zaq12wsx#";
        SSHClient sshClient = new SSHClient(hostname, port, username, password);
        SSHClientOutput output = sshClient.execCommand(sqoopScript);
        System.out.println("harvey:" + output.getText());
    }

}
