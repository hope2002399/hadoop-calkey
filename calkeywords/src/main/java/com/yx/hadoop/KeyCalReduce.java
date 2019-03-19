package com.iris.egrant;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KeyCalReduce extends Reducer<Text, IntWritable, Text, NullWritable> {

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

//		context.write(cotf, NullWritable.get());
	//	System.out.println(cotf.toString());
		context.write(new Text(cotf.toString()), NullWritable.get());
	}
}
