package com.iris.egrant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class KeyCalMapper  extends Mapper<LongWritable, Text, Text, IntWritable>{

	private static final String SPLIT = "##@@##";
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	String line = value.toString();  // k1;k2;k3;k4;k5..

    	String[] keySplit = line.split(";");
		Arrays.sort(keySplit);
    	int keySize = keySplit.length;
    	String outKey ="";
    	for (int i = 0; i <keySize; i++) {
 			for (int j = i+1; j < keySize; j++) {
				outKey = combine(keySplit[i],keySplit[j]);
				outKey= outKey + SPLIT +"2";
 				 context.write(new Text(outKey+""), new IntWritable(1));
				for (int k = j+1; k < keySize; k++) {
					outKey = combine(keySplit[i],keySplit[j],keySplit[k]);
					outKey= outKey + SPLIT +"3";
 					 context.write(new Text(outKey), new IntWritable(1));
					for (int m = k+1; m < keySize; m++) {
						outKey = combine(keySplit[i],keySplit[j],keySplit[k],keySplit[m]);
						outKey= outKey + SPLIT +"4";
 						 context.write(new Text(outKey), new IntWritable(1));
						 for (int n = m+1; n < keySize; n++) {
							 outKey = combine(keySplit[i],keySplit[j],keySplit[k],keySplit[m],keySplit[n]);
							 outKey= outKey + SPLIT +"5";
 							 context.write(new Text(outKey), new IntWritable(1));
						}
					}
				}
			}
		}
     } 
    /**
     * 拼接key 
     * @param str
     * @return
     */
    private String combine(String...str){
    	StringBuilder sb = new StringBuilder();
    	 for(String s:str){
    		 sb.append(";"+s) ;
    	 }
    	return sb.toString().substring(1) ; 
    }
  
}
