import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper 
		extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] queries = {"hackathon", "Dec", "Chicago", "Java"};
		List<String> words = Arrays.asList(value.toString().toLowerCase().split("\\W"));
		for (String q: queries) {
			if (words.contains(q.toLowerCase())) {
				context.write(new Text(q), new IntWritable(1));
			} else {	
				context.write(new Text(q), new IntWritable(0));
			}
		}	
	}
}
