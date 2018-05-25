package org.harvey.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.harvey.hadoop.util.HadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Random;

public class CreateHFileJob {
    protected static final Logger logger = LoggerFactory.getLogger(CreateHFileJob.class);

    public static class HFileMapper extends Mapper<Object, Object, ImmutableBytesWritable, KeyValue> {
        private HCatSchema schema;
        private byte[] cfBytes = Bytes.toBytes("cf");
        private ImmutableBytesWritable rowkey = new ImmutableBytesWritable();
        private KeyValue outputValue = null;
        private Random random = new Random();

        @Override
        public void setup(Context context) throws IOException {
            schema = HCatInputFormat.getTableSchema(context.getConfiguration());
        }

        @Override
        public void map(Object key, Object value, Context context) throws IOException, InterruptedException {
            HCatRecord record = (HCatRecord) value;
            //byte[] transId = Bytes.toBytes(record.getLong("trans_id", schema).intValue());
            byte[] transId = Bytes.toBytes(Integer.valueOf(record.getString("serial_no", schema)));
            transId = Bytes.add(Bytes.toBytes((short) random.nextInt(10)), transId); //5是不包含在内的，只产生0~4之间的数。
            rowkey.set(transId);

            List<String> columns = schema.getFieldNames();
            for (String column : columns) {
                Object o = record.get(column, schema);
                if (o != null) {
                    byte[] qBytes = Bytes.toBytes(column);
                    byte[] vBytes = Bytes.toBytes(o.toString());
                    outputValue = new KeyValue(transId, 0, transId.length, //
                            cfBytes, 0, cfBytes.length, //
                            qBytes, 0, qBytes.length, //
                            System.currentTimeMillis(), KeyValue.Type.Put, //
                            vBytes, 0, vBytes.length);
                    context.write(rowkey, outputValue);
                }
            }
        }

    }

    public static void main(String[] args) throws Exception {
        String output = "hdfs://master:8020/test/hfile";
        String schema = "clb_hospital_dw";
        String tableName = "fact_ipd_charge";
        Configuration conf = HadoopUtil.getLocalConfiguration();
        Job job = Job.getInstance(conf, "Create HFile");

        HCatInputFormat.setInput(job, schema, tableName);
        job.setInputFormatClass(HCatInputFormat.class);

        job.setJarByClass(CreateHFileJob.class);
        job.setMapperClass(HFileMapper.class);
        job.setReducerClass(KeyValueSortReducer.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(KeyValue.class);
        job.setOutputFormatClass(HFileOutputFormat2.class);

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(tableName));
        HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);

        Path outputPath = new Path(output);
        HadoopUtil.deletePath(conf, outputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
        //System.exit(job.waitForCompletion(true) ? 0 : 1);

        FsShell shell = new FsShell(conf);
        int exitCode = -1;
        int retryCount = 10;
        while (exitCode != 0 && retryCount >= 1) {
            exitCode = shell.run(new String[]{"-chmod", "-R", "777", output});
            retryCount--;
            Thread.sleep(5000);
        }

        if (exitCode != 0) {
            logger.error("Failed to change the file permissions: " + output);
            throw new IOException("Failed to change the file permissions: " + output);
        }

        String[] newArgs = new String[2];
        newArgs[0] = output;
        newArgs[1] = tableName;

        logger.info("Start to run LoadIncrementalHFiles");
        int ret = new LoadIncrementalHFiles(conf).run(newArgs);
        logger.info("End to run LoadIncrementalHFiles");
    }

}
