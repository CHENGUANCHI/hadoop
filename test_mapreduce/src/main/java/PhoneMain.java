import map.MapWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import phoneData.FlowBean;
import reduce.ReduceWritable;

import java.io.IOException;

public class PhoneMain {
    public static void main(String[] args)  throws IOException, ClassNotFoundException, InterruptedException{
        args = new String[]{"./src/main/resources/HTTP_20130313143750.dat","./src/main/resources/out/phone"};

        //1.获取job信息
        Configuration conf = new Configuration();
        Job job = new Job(conf, "phone main");

        //2.加载jar包
        job.setJarByClass(PhoneMain.class);

        //3.关联map和reduce
        job.setMapperClass(MapWritable.class);
        job.setReducerClass(ReduceWritable.class);

        //4.设置最终输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //5.设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //6.提交job任务
        //job.waitForCompletion(true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
