package avroMapReduce;

import maxtemp.MaxTemperature;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configured;
import org.apache.avro.mapred.AvroUtf8InputFormat;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.swing.text.DateFormatter;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AvroGenericMaxTemperature extends Configured implements Tool{
	private static final Schema SCHEMA = new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"WeatherRecord\", \"fields\":"
		      + "[{\"name\":\"year\", \"type\":\"int\"}, " +
		      "{\"name\":\"temperature\", \"type\":\"int\", \"order\": \"ignore\"}, " +
		      "{\"name\":\"stationId\", \"type\":\"string\", \"order\": \"ignore\"}]}");
	
	public static class MaxTemperatureMapper extends AvroMapper<Utf8, Pair<Integer, GenericRecord>> {
		private NcdcRecordParser parser = new NcdcRecordParser();
		private GenericRecord record = new GenericData.Record(SCHEMA);
		@Override
		public void map(Utf8 line, AvroCollector<Pair<Integer, GenericRecord>> collector, Reporter reporter) throws IOException{
			parser.parse(line.toString());
			if (parser.isValidTemperature()){
				record.put("year", parser.getYearInt());
				record.put("temperature", parser.getAirTemperature());
				record.put("stationId", parser.getStationId());
				collector.collect(new Pair<Integer, GenericRecord>(parser.getYearInt(), record));
			}
		}
	}

	public static class MaxTemperatureReducer extends AvroReducer<Integer, GenericRecord, GenericRecord>{
		@Override
		public void reduce(Integer key, Iterable<GenericRecord> values,
			AvroCollector<GenericRecord> collector, Reporter reporter){
			GenericRecord max = null;
			for(GenericRecord value : values){
				if  (max == null || (Integer) value.get("temperature") > (Integer) max.get("temperature")){
					max = newWeatherRecord(value);
				}
			}
		}
		private GenericRecord newWeatherRecord(GenericRecord value){
			GenericRecord record = new GenericData.Record(SCHEMA);
			record.put("year", value.get("year"));
			record.put("stationId", value.get("stationId"));
			record.put("temperature", value.get("temperature"));
			return record;
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
//		if (args.length != 2){
//			System.err.println("Usage Exception");
//			ToolRunner.printGenericCommandUsage(System.err);
//			return -1;
//		}
		String inPath = "/home/cloudera/hd/data/ncdc_sample.txt";
		String outPath = "/home/cloudera/hd/data/output_" + new SimpleDateFormat("yyyyMMdd_mmmm_ss").format(new Date());
		
	    JobConf conf = new JobConf(getConf(), getClass());
	    conf.setJobName("Max temperature");
		
		FileInputFormat.addInputPath(conf,  new Path(inPath));
		FileOutputFormat.setOutputPath(conf, new Path(outPath));
		
		AvroJob.setInputSchema(conf, Schema.create(Schema.Type.STRING));
	    AvroJob.setMapOutputSchema(conf,
	        Pair.getPairSchema(Schema.create(Schema.Type.INT), SCHEMA));
	    AvroJob.setOutputSchema(conf, SCHEMA);
	    conf.setInputFormat(AvroUtf8InputFormat.class);

	    AvroJob.setMapperClass(conf, MaxTemperatureMapper.class);
	    AvroJob.setReducerClass(conf, MaxTemperatureReducer.class);

	    JobClient.runJob(conf);
	    return 0;
	}
	
	public static void main (String[] args) throws Exception{
		int exitCode = ToolRunner.run(new AvroGenericMaxTemperature(), args);
		System.exit(exitCode);
	}
}
