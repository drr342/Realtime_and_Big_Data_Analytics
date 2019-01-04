import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper 
		extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		List<String> data = Arrays.asList(value.toString().split(" "));
		int links = data.size() - 2;
		double pr = Double.parseDouble(data.get(data.size() - 1)) / links;
		String targets = data.subList(1, links + 1).toString().replaceAll("[,]", "");
		context.write(new Text(data.get(0)), new Text(targets));
		for (int i = 1; i <= links; i++) {
			context.write(new Text(data.get(i)), new Text(String.valueOf(pr)));
		}
		
	}
}