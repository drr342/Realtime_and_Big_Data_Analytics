import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TrafficReducer extends Reducer<Text, Text, NullWritable, Text> {
	
//	private static String id;
//	
//	@Override
//	protected void setup(Context context) {
//		id = context.getConfiguration().get("ID");
//	}
	
	@Override
	public void reduce(Text key, Iterable<Text> valuesText, Context context) throws IOException, InterruptedException {
		Integer[] values = new Integer[2];
		int count = 0;
		int speedMin = Integer.MAX_VALUE;
		int speedMax = Integer.MIN_VALUE;
		Arrays.fill(values, 0);
		for (Text valueText : valuesText) {
			int[] tokens = Stream.of(valueText.toString().split(",")).mapToInt(Integer::parseInt).toArray();
			speedMin = Integer.min(speedMin, tokens[1]);
			speedMax = Integer.max(speedMax, tokens[1]);
			Arrays.setAll(values, i -> values[i] + tokens[i]);
			count++;
		}
		int bus = values[0];
		double speedAvg = (double) values[1] / (double) count;
//		String out = id + "," + key + "," + bus + "," + speedAvg + "," + speedMin + "," + speedMax;
		String out = key + "," + bus + "," + speedAvg + "," + speedMin + "," + speedMax;
		context.write(NullWritable.get(), new Text(out));
	}

}
