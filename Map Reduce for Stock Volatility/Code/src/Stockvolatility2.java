import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class Stockvolatility2 {
	
public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
		private String xia;
		private String tem;
		private String keymapp2;
		public void map(LongWritable key, Text value, Context context) throws IOException {
			try{
			String[] fields = value.toString().split("\t",2);
			tem = fields[0];
			xia = fields[1];
			String[] keymap2 = tem.toString().split(",");
			keymapp2 = keymap2[0];
			context.write(new Text(keymapp2),new Text(xia));
			}catch(IOException e){
				// TODO Auto-generated catch block
				e.printStackTrace();
			}catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static class Reducer2 extends Reducer<Text, Text, Text, Text> {
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
			  //context.write(key, new Text("NaN"));
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
		  //String volt = Double.toString(volatility);
		  context.write(key,new Text(volt));
		  arl.clear();
		  }
		  }
		}
	}
}