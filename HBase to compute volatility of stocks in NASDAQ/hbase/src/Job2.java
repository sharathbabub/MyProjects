import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class Job2 {
	
	public static class Map2 extends TableMapper<Text,Text> {
		 
public void map(ImmutableBytesWritable rowKey, Result columns, Context context)
		 throws IOException, InterruptedException {
          try {
		   String inKey = new String(rowKey.get());
		   String oKey = inKey.substring(0,inKey.indexOf('!')+7);
		   byte[] bprice = columns.getValue(Bytes.toBytes("stock"), Bytes.toBytes("adj"));
		   String sprice = new String(bprice);
		   context.write(new Text(oKey), new Text(sprice));
		  } catch (RuntimeException e){
		   e.printStackTrace();
		  }
		 }
	}
	
	public static class Reducer2 extends TableReducer<Text,Text,ImmutableBytesWritable>{
		private String startp,endp;
		private double xi;
		private List<String> valuesarray = new ArrayList<String>(); 
		public void reduce(Text key, Iterable<Text> values, Context context) 
		   throws IOException, InterruptedException {
		   try {
		   int sum = 0;
		   for (Text value : values) {
			   String val = value.toString();
			   valuesarray.add(val);
		   } 
		   Collections.sort(valuesarray);
	    startp = valuesarray.get(0).substring(2,valuesarray.get(0).length());
		endp = valuesarray.get(valuesarray.size()-1).substring(2,valuesarray.get(valuesarray.size()-1).length());
				   double startprice = Double.parseDouble(startp);
				   double endprice = Double.parseDouble(endp);
				   xi =  (endprice - startprice)/startprice;
				   DecimalFormat frm = new DecimalFormat("#0.00000");
				   String xit = frm.format(xi);
				   valuesarray.clear();
				   String ka = key.toString();
		   Put pa = new Put(ka.getBytes());
		   pa.add(Bytes.toBytes("stock"), Bytes.toBytes("xi"), Bytes.toBytes(xit));
		   context.write(new ImmutableBytesWritable(ka.getBytes()), pa);
		  } catch (Exception e) {
		   e.printStackTrace();
		  }
	   }
	}
}