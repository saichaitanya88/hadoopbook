package delete;

import java.net.URI;

import listStatus.CustomPathFilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class Delete {
	//hdfs://localhost/user/cloudera/ncdc_sample.txt
	public static void main(String[] args) throws Exception{
		//BasicConfigurator.configure();
		String uri = "hdfs://localhost";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		boolean deleteSuccess = fs.delete(new Path("hdfs://localhost/user/cloudera/ncdc_sample.txt"), false);
		if (deleteSuccess)
			System.out.println("Delete Success");
		else
			System.out.println("Delete Failed");
	}
}
