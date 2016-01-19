package codecs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

public class FileDecompressor {
	public static void main (String[] args) throws IOException{
		String uri = "/home/cloudera/hd/data/ncdc_tmp/ftp.ncdc.noaa.gov/pub/data/noaa/2000/719043-99999-2000.gz";
		String outputPath = "/home/cloudera/hd/data/test";
		String codecName = "org.apache.hadoop.io.compress.GzipCodec";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		
		Path inputPath = new Path(uri);
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		
		CompressionCodec codec = factory.getCodecByName(codecName);
		
		if (codec == null){
			System.err.println("No codec found for " + codecName);
			System.exit(1);
		}
		
		String outputUri = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());
		InputStream in = null;
		OutputStream out = null;

		try{
			in = codec.createInputStream(fs.open(inputPath));
			out = fs.create(new Path(outputPath));
			IOUtils.copyBytes(in, out, 4096, true);
		}
		finally{
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
		}
	}
}
