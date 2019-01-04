import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BusinessReducer extends Reducer<Text, IntWritable, NullWritable, Text> {
	
//	private static String id;
	
//	@Override
//	protected void setup(Context context) {
//		id = context.getConfiguration().get("ID");
//	}
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		Iterator<IntWritable> i = values.iterator();
		while(i.hasNext()) {
			i.next();
			count++;
		}
//		String out = id + "," + key + "," + count;
		String out = key + "," + count;
		context.write(NullWritable.get(), new Text(out));
	}

}
