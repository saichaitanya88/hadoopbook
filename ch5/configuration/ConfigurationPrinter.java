package configuration;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

import java.util.Map.Entry;

public class ConfigurationPrinter extends Configured implements Tool{

	static {
		Configuration.addDefaultResource("hdfs-default.xml");
		Configuration.addDefaultResource("hdfs-site.xml");
		Configuration.addDefaultResource("mapred-default.xml");
		Configuration.addDefaultResource("mapred-site.xml");
	}
	
	@Override
	public int run(String[] arg) throws Exception {
		Configuration conf = new Configuration();
		for(Entry<String,String> entry: conf){
			System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
		}
		return 0;
	}
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new ConfigurationPrinter(), args);
		System.exit(exitCode);
	}

}
