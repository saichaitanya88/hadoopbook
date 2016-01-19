package urlcat;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.BasicConfigurator;

public class URLCat2 {
	public static void main(String[] args) throws Exception{
		BasicConfigurator.configure();
		String url = "hdfs://localhost/user/cloudera/1901";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(url), conf);
		FSDataInputStream in = null;
		boolean seekBack = true;
		try{
			in = fs.open(new Path(url));
			IOUtils.copyBytes(in, System.out, 4096, false);
			// seek
			if (seekBack)
			{
				in.seek(0);
				IOUtils.copyBytes(in, System.out, 4096, false);
			}
		}
		finally{
			IOUtils.closeStream(in);
		}
	}
}
