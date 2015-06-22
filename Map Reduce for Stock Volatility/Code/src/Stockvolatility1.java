import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Stockvolatility1 {
	
public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
		private String date,adjclose,ym,day;
public void map(LongWritable key, Text value, Context context) throws IOException {
	FileSplit fileSplit = (FileSplit)context.getInputSplit();
    String fileName = fileSplit.getPath().getName();
    if (key.get()>0) {
    	try {
			String[] fields = value.toString().split(",");
			date = fields[0];
			adjclose = fields[6];
			ym = date.substring(0,date.indexOf("-",5));
			day = date.substring(8,10);
			String stockname = fileName.substring(0,fileName.indexOf("."));
			String keymap1 = stockname+","+ym;
			String valmap1 = day+adjclose;
			context.write(new Text(keymap1),new Text(valmap1));
			}catch(IOException e){
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    }
		}
}

public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
	private String startp,endp;
	private double xi;
	private List<String> valuesarray = new ArrayList<String>();
	public void reduce(Text key, Iterable<Text> values, Context context) 
	throws IOException, InterruptedException {
		   for (Text value:values){
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
		   context.write(new Text(key),new Text(xit));
		   valuesarray.clear();
	}
}
}