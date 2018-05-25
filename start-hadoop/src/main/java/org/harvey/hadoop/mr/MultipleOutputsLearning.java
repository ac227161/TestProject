package org.harvey.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.harvey.hadoop.util.HadoopUtil;

import java.io.IOException;

public class MultipleOutputsLearning {

    public static class MultiOutputMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (value != null && value.getLength() > 0) {
                String line = value.toString().trim();
                String[] arr = line.split(",");
                context.write(new IntWritable(Integer.parseInt(arr[0])), value);
            }
        }
    }

    public static class MultiOutputReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
        private MultipleOutputs mos = null;

        @Override
        protected void setup(Context context) {
            mos = new MultipleOutputs(context);
            System.out.println("TaskAttemptID: " + context.getTaskAttemptID().toString());
            System.out.println("ReduceID: " + context.getTaskAttemptID().getTaskID().getId());
        }

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text text : values) {
                /*
                 * multipleOutputs.write(key, value, baseOutputPath)方法的第三个参数表明该输出所在的目录
                 * 如果baseOutputPath不包含文件分隔符"/"，那么输出的文件格式为baseOutputPath-r-nnnnn（name-r-nnnnn)
                 * 如果包含文件分隔符"/"，例如baseOutputPath="029070-99999/1901/part"，那么输出文件则为029070-99999/1901/part-r-nnnnn
                 */
                mos.write("KeySpilt", NullWritable.get(), text, key.toString() + "/"); // 1101/-r-00000
                mos.write("AllPart", NullWritable.get(), text);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (null != mos) {
                mos.close();
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HadoopUtil.getLocalConfiguration();
        Job job = Job.getInstance(conf, "MultipleOutputs Sample");

        FileInputFormat.addInputPath(job, new Path("/test/multi/in"));

        Path outputPath = new Path("/test/multi/out");
        HadoopUtil.deletePath(conf, outputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        MultipleOutputs.addNamedOutput(job, "KeySpilt", TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "AllPart", TextOutputFormat.class, NullWritable.class, Text.class);

        job.setMapperClass(MultiOutputMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MultiOutputReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(2);

        // prevent to create zero-sized default output
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        job.waitForCompletion(true);
    }

}
