package fileCopy;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class FileCopyWithProgress {
	public static void main(String[] args) throws Exception{
		String localSrc = "/home/cloudera/hd/data/ncdc_sample.txt";
		String dst = "hdfs://localhost/user/cloudera/ncdc_sample.txt";
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		OutputStream out = fs.create(new Path(dst), new Progressable(){
			public void progress(){
				System.out.println("PROGRESSING...");
			}
		});
		IOUtils.copyBytes(in,  out, 4096, true);
		System.out.println("Done!");
	}
}
