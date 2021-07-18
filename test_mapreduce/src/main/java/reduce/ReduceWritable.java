package reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import phoneData.FlowBean;

import java.io.IOException;

public class ReduceWritable extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //1.定义两个计数器，计算每个用户的上传流量、下载流量
        long sumupflow = 0;
        long sumdownflow = 0;

        //2.累加的号的流量和
        for (FlowBean f: values) {
            sumupflow+=f.getUpFlow();
            sumdownflow+=f.getDownFlow();
        }

        //3.输出
        context.write(key,new FlowBean(sumupflow,sumdownflow));
    }
}
