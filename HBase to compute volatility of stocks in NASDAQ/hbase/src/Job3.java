import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class Job3 {
	public static class Map3 extends TableMapper<Text,Text> {
		public void map(ImmutableBytesWritable rowKey, Result columns, Context context)
				 throws IOException, InterruptedException {
			try {
				String inKey = new String(rowKey.get());
				String oKey = inKey.substring(0,inKey.indexOf('!'));
				byte[] bprice = columns.getValue(Bytes.toBytes("stock"), Bytes.toBytes("xi"));
				String sprice = new String(bprice);
				context.write(new Text(oKey), new Text(sprice));
			}catch (RuntimeException e){
				   e.printStackTrace();
			  }
	}
}
	
	public static class Reducer3 extends TableReducer<Text,Text,ImmutableBytesWritable>{
		private List<Double> arl = new ArrayList<Double>();
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException{
			 double sumxi = 0;
			  int N = 0;
			  double volatility;
			  double xbar;
			  for (Text val:values){
				  String valu = val.toString();
				double va = Double.parseDouble(valu);
				arl.add(va);
				sumxi += va;
				N += 1;
			  }
			  xbar = sumxi/N;
			  double sumvol = 0;
			  for (Double am:arl){
				sumvol += Math.pow((am-xbar),2);
			  }
			  int NN = N-1;
			  if (NN == 0 ){
				  return;
			  }
			  else {
			  volatility = Math.sqrt(sumvol/NN);
			  if (volatility == 0){
				  return;
			  }
			  else {
			  DecimalFormat frm = new DecimalFormat("##.#########################");  
			  String volt = frm.format(volatility);
			  arl.clear();
			  String kaa = key.toString();
			  Put pa = new Put(kaa.getBytes());
			  pa.add(Bytes.toBytes("stock"), Bytes.toBytes("return"), Bytes.toBytes(volt));
			  context.write(new ImmutableBytesWritable(kaa.getBytes()), pa);
			  }
			  }
			}
		}
}
