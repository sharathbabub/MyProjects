import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Job1 {
	/**
	 * @author ruhansa
	 * read files line by line, put the data into hbase style table
	 * input: <key, value>, key: line number, value: line
	 * output: <key, value>, key: rowid, value: hbase row content
	 */
	public static class Map1 extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{
		
		public void map(LongWritable key, Text value, Context context){
			String line = value.toString(); //receive one line
			String element[] = null;
			element = line.split(",");
			if (element[0].trim().compareTo("Date") != 0){
				String dates[]= element[0].split("-");
				String val1 = dates[2]+element[6];
				byte[] vals1 = Bytes.toBytes(val1);
				String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
				String stockName = fileName.substring(0, fileName.length()-4);
				String key1 = stockName+"!"+dates[0]+dates[1];
				byte[] keys1 = Bytes.toBytes(key1);
				byte[] rowid = Bytes.toBytes(key1 + key.toString());
				Put p = new Put(rowid);
				p.add(Bytes.toBytes("stock"), Bytes.toBytes("adj"), vals1);
				try {
					context.write(new ImmutableBytesWritable(rowid),p);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
}