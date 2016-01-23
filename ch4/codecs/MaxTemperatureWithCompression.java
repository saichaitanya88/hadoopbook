package codecs;

import java.io.IOException;

import maxtemp.MaxTemperature;
import maxtemp.MaxTemperatureMapper;
import maxtemp.MaxTemperatureReducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperatureWithCompression {
	public static void main (String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{
		Job job = new Job();
		job.setJarByClass(MaxTemperature.class);
		
		String inputPath = "/home/cloudera/hd/data/ncdc_tmp/ftp.ncdc.noaa.gov/pub/data/noaa/2000/719043-99999-2000.gz";
		String outputPath = "/home/cloudera/hd/data/output";
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileOutputFormat.setCompressOutput(job,  true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		job.setCombinerClass(MaxTemperatureReducer.class);
		
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}
}
