import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class Main{

	public static void main(String[] args){
		Configuration conf = HBaseConfiguration.create();
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("raw"));
			tableDescriptor.addFamily(new HColumnDescriptor("stock"));
			if (admin.isTableAvailable("raw")){
				admin.disableTable("raw");
				admin.deleteTable("raw");
			}
			admin.createTable(tableDescriptor);
						
			HTableDescriptor tableDescriptor2 = new HTableDescriptor(TableName.valueOf("raw1"));
			tableDescriptor2.addFamily(new HColumnDescriptor("stock"));
			if (admin.isTableAvailable("raw1")){
				admin.disableTable("raw1");
				admin.deleteTable("raw1");
			}
			admin.createTable(tableDescriptor2);
			
			HTableDescriptor tableDescriptor3 = new HTableDescriptor(TableName.valueOf("raw2"));
			tableDescriptor3.addFamily(new HColumnDescriptor("stock"));
			if (admin.isTableAvailable("raw2")){
				admin.disableTable("raw2");
				admin.deleteTable("raw2");
			}
			admin.createTable(tableDescriptor3);
			
			HTableDescriptor tableDescriptor4 = new HTableDescriptor(TableName.valueOf("raw3"));
			tableDescriptor4.addFamily(new HColumnDescriptor("stock"));
			if (admin.isTableAvailable("raw3")){
				admin.disableTable("raw3");
				admin.deleteTable("raw3");
			}
			admin.createTable(tableDescriptor4);
			
			Scan scan = new Scan();
		    scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		    scan.setCacheBlocks(false);  // don't set to true for MR jobs
		    scan.addFamily(Bytes.toBytes("stock"));
		    
			Job job1 = Job.getInstance();
			job1.setJarByClass(Job1.class);
		    Job job2 = Job.getInstance();
		    job2.setJarByClass(Job2.class);
		    Job job3 = Job.getInstance();
		    job3.setJarByClass(Job3.class);
		    Job job4 = Job.getInstance();
		    job4.setJarByClass(Job4.class);
		     
		    job1.setMapperClass(Job1.Map1.class);
		    job2.setMapperClass(Job2.Map2.class);
		    job3.setMapperClass(Job3.Map3.class);
		    job4.setMapperClass(Job4.Map4.class);
			FileInputFormat.addInputPath(job1, new Path(args[0]));
			job1.setInputFormatClass(TextInputFormat.class);
			TableMapReduceUtil.initTableReducerJob("raw", null, job1);
			job1.setNumReduceTasks(0);
			job1.waitForCompletion(true);
			
			TableMapReduceUtil.initTableMapperJob(
					"raw",      // input table
					scan,	          // Scan instance to control CF and attribute selection
					Job2.Map2.class,   // mapper class
					Text.class,	          // mapper output key
					Text.class,	          // mapper output value
					job2);
			TableMapReduceUtil.initTableReducerJob("raw1", Job2.Reducer2.class, job2);
			//job2.setNumReduceTasks(1);
			job2.waitForCompletion(true);
			
			TableMapReduceUtil.initTableMapperJob(
					"raw1",      // input table
					scan,	          // Scan instance to control CF and attribute selection
					Job3.Map3.class,   // mapper class
					Text.class,	          // mapper output key
					Text.class,	          // mapper output value
					job3);
			TableMapReduceUtil.initTableReducerJob("raw2", Job3.Reducer3.class, job3);
			//job3.setNumReduceTasks(1);
			job3.waitForCompletion(true);
			
			TableMapReduceUtil.initTableMapperJob(
					"raw2",      // input table
					scan,	          // Scan instance to control CF and attribute selection
					Job4.Map4.class,   // mapper class
					Text.class,	          // mapper output key
					Text.class,	          // mapper output value
					job4);
			TableMapReduceUtil.initTableReducerJob("raw3", Job4.Reducer4.class, job4);
			//job4.setNumReduceTasks(1);
			job4.waitForCompletion(true);
			admin.close();			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}