package urlcat;

import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.BasicConfigurator;

public class URLCat {
	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	public static void main(String[] args) throws Exception{
		BasicConfigurator.configure();
		InputStream in = null;
		String url = "hdfs://localhost/user/cloudera/1901";
		try{
			in = new URL(url).openStream();
			IOUtils.copyBytes(in, System.out, 4096, false);
		}
		finally{
			IOUtils.closeStream(in);
		}
	}
}
