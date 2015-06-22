import java.io.IOException;
import java.util.TreeSet;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class Stockvolatility3 {
public static class Map3 extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) 
		throws IOException {
			try{
				String[] field = value.toString().split("\t",2); 
				String keymap3 = field[0];
			String valmap3 = field[1];
				String x = valmap3+"!"+keymap3;
				context.write(new Text("A"),new Text(x));
			}catch(IOException e){
				// TODO Auto-generated catch block
				e.printStackTrace();
			}catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	}

public static class Reducer3 extends Reducer<Text, Text, Text, Text> {
     TreeSet<String> treeset = new TreeSet<String>();
     int c = 0;
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
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
                context.write(new Text(va[0]), new Text(va[1]));

            }
            else if(c > limit) {
                va = itr.next().toString().split("!");
                context.write(new Text(va[0]), new Text(va[1]));
            }
            else {
            	itr.next();
            }
        }
    }
}
}
