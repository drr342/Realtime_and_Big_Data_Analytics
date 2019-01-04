import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer
		extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		double sum = 0.0;
		String output = "";
		for (Text value : values) {
			if (value.toString().matches("\\[.+\\]"))
				output += value.toString().replaceAll("[\\[\\]]", "");
			else
				sum += Double.parseDouble(value.toString());
		}
		output += " " + sum;
		context.write(key, new Text(output));
	}
}