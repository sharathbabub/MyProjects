import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class Job4 {
	public static class Map4 extends TableMapper<Text,Text> {
		 
		public void map(ImmutableBytesWritable rowKey, Result columns, Context context)
				 throws IOException, InterruptedException {
		          try {
		       String oKey = new String(rowKey.get());
   byte[] bprice = columns.getValue(Bytes.toBytes("stock"), Bytes.toBytes("return"));
		   		   String sprice = new String(bprice);
		   		   String ss = sprice+"!"+oKey;
		   		   context.write(new Text("A"), new Text(ss));  
		          }catch (RuntimeException e){
		   		   e.printStackTrace();
				  }
		}
	}
	
	public static class Reducer4 extends TableReducer<Text,Text,ImmutableBytesWritable>{
		TreeSet<String> treeset = new TreeSet<String>();
	     int c = 0;
	     public void reduce(Text key, Iterable<Text> values, Context context) 
	  		   throws IOException, InterruptedException {
	    	 for (Text val: values){
	         	treeset.add(val.toString());
	         }
	    	 int treesize = treeset.size();
	         int limit = treesize-10;
	         Iterator itr = treeset.iterator();
	         while(itr.hasNext()){
	             String[] va;
	             c++;
	             if(c < 11) {
	                 va = itr.next().toString().split("!");
	                 Put pa = new Put(va[1].getBytes());
	                 //System.out.println(va[0]+" and "+va[1]);
pa.add(Bytes.toBytes("stock"), Bytes.toBytes("volatility"), Bytes.toBytes(va[0]));
	 	   context.write(new ImmutableBytesWritable(va[1].getBytes()), pa);

	             }
	             else if(c > limit) {
	                 va = itr.next().toString().split("!");
	                //System.out.println(va[0]+" and "+va[1]);
	                 Put pa = new Put(va[1].getBytes());
pa.add(Bytes.toBytes("stock"), Bytes.toBytes("volatility"), Bytes.toBytes(va[0]));
		  context.write(new ImmutableBytesWritable(va[1].getBytes()), pa);
	             }
	             else {
	             	itr.next();
	             }
	         }
	     }
	}
}
