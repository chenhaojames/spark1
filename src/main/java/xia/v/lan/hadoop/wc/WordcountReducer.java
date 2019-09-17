package xia.v.lan.hadoop.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author chenhao
 * @description <p>
 * created by chenhao 2019/9/17 11:06
 * KEYIN , VALUEIN 对应mapper输出的KEYOUT, VALUEOUT类型
 * KEYOUT，VALUEOUT 对应自定义reduce逻辑处理结果的输出数据类型 KEYOUT是单词 VALUEOUT是总次数
 */
public class WordcountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for(IntWritable v : values){
            count += v.get();
        }
        context.write(key,new IntWritable(count));
    }
}
