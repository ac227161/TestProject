package org.harvey.hadoop.kylin;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.harvey.hadoop.util.HadoopUtil;

import java.io.IOException;
import java.util.Set;

public class FactDistinctColumnsJob {
    public static class FactDistinctColumnsMapper extends Mapper<Object, Object, Text, LongWritable> {
        private HCatSchema schema;

        public void setup(Context context) throws IOException {
            schema = HCatInputFormat.getTableSchema(context.getConfiguration());
        }

        public void map(Object key, Object value, Context context) throws IOException, InterruptedException {
            Text result = new Text();
            LongWritable result2 = new LongWritable();
            HCatRecord record = (HCatRecord) value;
            String val = record.getString("ops_region", schema);
            result.set(val);
            result2.set(record.getLong("seller_id", schema));
            context.write(result, result2);
        }
    }

    public static class FactDistinctColumnsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable result = new LongWritable();
        private Set<Long> set = Sets.newHashSet();

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            for (LongWritable val : values) {
                set.add(val.get());
            }
            result.set(set.size());
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HadoopUtil.getLocalConfiguration();
        Job job = Job.getInstance(conf, "fact distinct count");

        HCatInputFormat.setInput(job, "default", "kylin_sales");
        job.setInputFormatClass(HCatInputFormat.class);

        job.setJarByClass(FactDistinctColumnsJob.class);
        job.setMapperClass(FactDistinctColumnsMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //job.setCombinerClass(FactDistinctColumnsReducer.class);
        job.setReducerClass(FactDistinctColumnsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setNumReduceTasks(2);

        String output = "hdfs://master:8020/test/out3";
        Path outputPath = new Path(output);
        FileOutputFormat.setOutputPath(job, outputPath);

        HadoopUtil.deletePath(conf, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
