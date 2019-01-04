import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TrainReducer extends Reducer<Text, IntWritable, NullWritable, Text> {
	
//	private static String id;
//	
//	@Override
//	protected void setup(Context context) {
//		id = context.getConfiguration().get("ID");
//	}
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//		int[] values = new int[8];
//		Arrays.fill(values, 0);
//		for (Text valueText : valuesText) {
//			int[] tokens = Stream.of(valueText.toString().split(",")).mapToInt(Integer::parseInt).toArray();
//			Arrays.setAll(values, i -> values[i] + tokens[i]);
//		}
//		String out = id + "," + key + "," + Arrays.toString(values).replaceAll("[\\[\\] ]", "");
//		String out = key + "," + Arrays.toString(values).replaceAll("[\\[\\] ]", "");
		int out = 0;
		for (IntWritable value: values) {
			out += value.get();
		}
		context.write(NullWritable.get(), new Text(key + "," + out));
	}

}
