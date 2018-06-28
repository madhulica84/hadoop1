package com.madhu.MapReduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class SeismicAnalysis {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable> {

@Override
public void map(LongWritable key, Text value, OutputCollector<Text,FloatWritable> output, Reporter reporter)
		throws IOException {

	//skip first row which has only headers
	if(key.get() == 0) {
		return;
	}
	
	String line = value.toString();
	
	StringTokenizer tokenizer = new StringTokenizer(line,",");

		
	//Check the data fields, the 8th and 11th fields are required
	for(int i =0;i<8;i++) {
		tokenizer.nextToken();
	}
	
	FloatWritable magnitude = new FloatWritable(Float.valueOf(tokenizer.nextToken()));
	tokenizer.nextToken();
	tokenizer.nextToken();
	Text region = new Text(tokenizer.nextToken());

	output.collect(region,magnitude);
}
}


public static class Reduce extends MapReduceBase implements
	Reducer<Text, FloatWritable, Text, FloatWritable> {

@Override
public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter)
		throws IOException {

	float maxMag = 0;//initialize
	
	while(values.hasNext()) {
		float x = values.next().get();
		if(maxMag < x) {
			maxMag =x;
		}
	}

	output.collect(key, new FloatWritable(maxMag));
}
}

public static void main(String[] args) throws Exception {

JobConf conf = new JobConf(SeismicAnalysis.class);
conf.setJobName("seismic");

// Forcing program to run 5 reducers
conf.setNumReduceTasks(1);

conf.setMapperClass(Map.class);
conf.setCombinerClass(Reduce.class);
conf.setReducerClass(Reduce.class);
//conf.setPartitionerClass(MyPartitioner.class);

conf.setOutputKeyClass(Text.class);
conf.setOutputValueClass(FloatWritable.class);

conf.setInputFormat(TextInputFormat.class);
conf.setOutputFormat(TextOutputFormat.class);

 FileInputFormat.setInputPaths(conf, new Path(args[0]));
 FileOutputFormat.setOutputPath(conf, new Path(args[1]));
 
JobClient.runJob(conf);
}
	
	
	
}
