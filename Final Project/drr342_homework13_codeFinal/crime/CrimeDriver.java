import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CrimeDriver extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new CrimeDriver(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(new Configuration(), args).getRemainingArgs();
		if (otherArgs.length < 3) {
			System.err.println("Usage: CrimeDriver <in> <out> <types>");
			System.exit(1);
		}

        Path inputPath = new Path(otherArgs[0]);
        Path dataPath = new Path (otherArgs[1]);
        String types = otherArgs[2];
        URI mapUri = new URI(types + "#MAP");

		Configuration crimeJobConf = getConf();
//		crimeJobConf.set("ID", "01");

		Job crimeJob = Job.getInstance(crimeJobConf, "Crime Job");
		crimeJob.addCacheFile(mapUri);
		crimeJob.setJarByClass(CrimeDriver.class);
		crimeJob.setNumReduceTasks(1);
		crimeJob.setMapperClass(CrimeMapper.class);
		crimeJob.setReducerClass(CrimeReducer.class);
		crimeJob.setMapOutputKeyClass(Text.class);
		crimeJob.setMapOutputValueClass(Text.class);
		crimeJob.setOutputKeyClass(NullWritable.class);
		crimeJob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(crimeJob, inputPath);
		FileOutputFormat.setOutputPath(crimeJob, dataPath);
		return crimeJob.waitForCompletion(true) ? 0 : 1;
	}
}
