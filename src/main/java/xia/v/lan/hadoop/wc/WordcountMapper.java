package xia.v.lan.hadoop.wc;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author chenhao
 * @description <p>
 * created by chenhao 2019/9/17 10:46
 * /**
 *  * KEYIN:默认情况下，是mr框架所读到的一行文本的起始偏移量，Long;
 *  * 在hadoop中有自己的更精简的序列化接口，所以不直接用Long，而是用LongWritable
 *  * VALUEIN:默认情况下，是mr框架所读到的一行文本内容，String;此处用Text
 *  * KEYOUT:是用户自定义逻辑处理完成之后输出数据中的key,在此处是单词，String；此处用Text
 *  * VALUEOUT，是用户自定义逻辑处理完成之后输出数据中的value，在此处是单词次数，Integer，此处用IntWritable
 *  * @author Administrator
 *
 */
public class WordcountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        for(String word : words){
            context.write(new Text(word),new IntWritable(1));
        }
    }
}
