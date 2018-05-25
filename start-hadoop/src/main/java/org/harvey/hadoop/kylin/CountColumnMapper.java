package org.harvey.hadoop.kylin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

import java.io.IOException;

public class CountColumnMapper extends Mapper<Object, Object, Text, IntWritable> {
    private HCatSchema schema;
    private final static IntWritable one = new IntWritable(1);

    public void setup(Mapper.Context context) throws IOException {
        schema = HCatInputFormat.getTableSchema(context.getConfiguration());
    }

    public void map(Object key, Object value, Mapper.Context context) throws IOException, InterruptedException {
        Text result = new Text();
        HCatRecord record = (HCatRecord) value;
        String val = record.getString("ops_region", schema);
        //String val = record.get(11) == null ? "" : record.get(11).toString();
        System.out.println("harvey:" + val);
        result.set(val);
        context.write(result, one);
            /*
            String[] arr = new String[record.size()];
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < arr.length; i++) {
                Object o = record.get(i);
                arr[i] = (o == null) ? null : o.toString();
                sb.append((o == null) ? "" : o.toString() + "|");
            }
            System.out.println(sb.toString());
            result.set(sb.toString());
            context.write(key, result);
            */
    }
}
