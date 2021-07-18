package map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import phoneData.FlowBean;

import java.io.IOException;

public class MapWritable extends Mapper<LongWritable, Text, Text, FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取数据
        String line = value.toString();

        //2.切分数据
        String[] fields = line.split("\t");

        //3.获取上传流量
        long upflow = Long.parseLong(fields[fields.length - 3]);
        long downflow = Long.parseLong(fields[fields.length - 2]);

        //4.输出
        context.write(new Text(fields[1]),new FlowBean(upflow,downflow));
    }
}
