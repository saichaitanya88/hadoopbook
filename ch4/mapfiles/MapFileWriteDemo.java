package mapfiles;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.common.IOUtils;

public class MapFileWriteDemo {

	private static final String[] DATA = {
		"One, two, buckle my shoe",
		"Three, four, shut the door",
		"Five, six, pick up sticks",
		"Seven, eight, lay them straight",
		"Nine, ten, big fat hen"
	};
	
	public static void main(String[] args) throws IOException{
		String uri = "/home/cloudera/hd/data/numbers.map";//args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
		
		IntWritable key = new IntWritable();
		Text value = new Text();
		MapFile.Writer writer = null;
		try{
			writer = new MapFile.Writer(conf, fs,  uri, key.getClass(), value.getClass());
			for(int i = 0; i < 1024; i++){
				key.set(i + 1);
				value.set(DATA[i % DATA.length]);
				writer.append(key, value);
			}
		}
		finally{
			IOUtils.closeStream(writer);
		}
	}
	
}
