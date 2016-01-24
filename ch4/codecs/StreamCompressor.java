package codecs;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class StreamCompressor {
	public static void main(String[] args) throws ClassNotFoundException, IOException{
		String codecClassName = "org.apache.hadoop.io.compress.GzipCodec";
		Class<?> codecClass = Class.forName(codecClassName);
		String localSrc = "/home/cloudera/hd/data/test";
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		Configuration conf = new Configuration();
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
		CompressionOutputStream out = codec.createOutputStream(System.out);
		IOUtils.copyBytes(in, out, 4096, false);
		out.finish();
	}
}
