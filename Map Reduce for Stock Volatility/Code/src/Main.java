import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Main {
	public static void main(String[] args) throws Exception {		
		long start = new Date().getTime();
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		
	     Job job1 = Job.getInstance();
	     job1.setJarByClass(Stockvolatility1.class);
	     Job job2 = Job.getInstance();
	     job2.setJarByClass(Stockvolatility2.class);
	     Job job3 = Job.getInstance();
	     job3.setJarByClass(Stockvolatility3.class);

		System.out.println("\n**********Mapreduce Stock->Start**********\n");
		
		job1.setJarByClass(Stockvolatility1.class);
		job1.setMapperClass(Stockvolatility1.Map1.class);
		job1.setReducerClass(Stockvolatility1.Reducer1.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
	
		job2.setJarByClass(Stockvolatility2.class);
		job2.setMapperClass(Stockvolatility2.Map2.class);
		job2.setReducerClass(Stockvolatility2.Reducer2.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		
		job3.setJarByClass(Stockvolatility3.class);
		job3.setMapperClass(Stockvolatility3.Map3.class);
		job3.setReducerClass(Stockvolatility3.Reducer3.class);
		
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setNumReduceTasks(1);
        
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("In1"+args[1]));
		FileInputFormat.addInputPath(job2, new Path("In1"+args[1]));
		FileOutputFormat.setOutputPath(job2, new Path("In2"+args[1]));
		FileInputFormat.addInputPath(job3, new Path("In2"+args[1]));
		FileOutputFormat.setOutputPath(job3, new Path("Out"+args[1]));
	
		job1.waitForCompletion(true);
        job2.waitForCompletion(true);
		boolean status = job3.waitForCompletion(true);
		if (status == true) {
			long end = new Date().getTime();
			System.out.println("\nJob took " + (end-start)/1000 + " seconds\n");
		}
		System.out.println("\n**********Mapreduce Stock->End**********\n");	
}
}