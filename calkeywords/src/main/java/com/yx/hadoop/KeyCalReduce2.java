package com.iris.egrant;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 原先 回写数据库版本的
 * 出现oom 暂时不采用
 */
@Deprecated
public class KeyCalReduce2 extends Reducer<Text, IntWritable, KeyCotfVO, NullWritable> {

	private static final String SPLIT = "##@@##";

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		//求总和==cotf
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		} 
		String keyS = key.toString();
		String[] keySize = keyS.split(SPLIT); // k1 组合词  k2 组合词数量
		KeyCotfVO cotf = new KeyCotfVO();
		cotf.setTmp_key_cotf(keySize[0]);
		cotf.setKey_combine_size(Integer.parseInt(keySize[1]));
		cotf.setCotf(sum);
		context.write(cotf, NullWritable.get());
	}
}
