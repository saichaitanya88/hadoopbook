package maxtemp;


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class MaxTemperature {
	public static void main(String[] args) throws Exception {
//		if (args.length != 2) {
//			System.err
//					.println("Usage: MaxTemperature <input path> <output path>");
//			System.exit(-1);
//		}
		BasicConfigurator.configure();
		String input = "/home/cloudera/hd/data/ncdc_data/test/";
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		String output = "/home/cloudera/hd/data/output" + dateFormat.format(new Date());
		Job job = new Job();
		job.setJarByClass(MaxTemperature.class);
		job.setJobName("Max temperature");

		FileInputFormat.addInputPath(job, new Path(input));

		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
