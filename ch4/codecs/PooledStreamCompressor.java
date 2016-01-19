package codecs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.util.ReflectionUtils;

public class PooledStreamCompressor {
	public static void main (String[] args) throws ClassNotFoundException, IOException{
		String codecClassName = "org.apache.hadoop.io.compress.GzipCodec";
		Class<?> codecClass = Class.forName(codecClassName);
		Configuration conf = new Configuration();
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
		Compressor compressor = null;
		try{
			// Gets Compressor Reference from the pool for usage
			compressor = CodecPool.getCompressor(codec);
			CompressionOutputStream out = codec.createOutputStream(System.out, compressor);
			IOUtils.copyBytes(System.in, out, 4096, true);
		}
		finally{
			// return Compressor to the pool after usage
			CodecPool.returnCompressor(compressor);
		}
	}
}
