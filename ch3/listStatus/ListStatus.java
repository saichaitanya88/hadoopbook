package listStatus;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class ListStatus {
	public static void main(String[] args) throws Exception{
		//BasicConfigurator.configure();
		String uri = "hdfs://localhost";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path[] paths = new Path[2];
		paths[0] = new Path("hdfs://localhost/user/");
		paths[1] = new Path("hdfs://localhost/user/cloudera");
		FileStatus[] statuses = fs.listStatus(paths);
		for(FileStatus status : statuses){
			String line = "Owner: " + status.getOwner() + "\n"; 
			line += "Path: " + status.getPath() + "\n";
			line += "Permissions: " + status.getPermission() + "\n";
			System.out.println(line);
			System.out.println();
		}
		System.out.println("Using PathFilter");
		// Using PathFilter
		PathFilter filter = new CustomPathFilter();
		statuses = fs.globStatus(new Path("hdfs://localhost/user/*"), filter);
		for(FileStatus status : statuses){
			String line = "Owner: " + status.getOwner() + "\n"; 
			line += "Path: " + status.getPath() + "\n";
			line += "Permissions: " + status.getPermission() + "\n";
			System.out.println(line);
			System.out.println();
		}
	}
}

