import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TrainDriver extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TrainDriver(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {		
		String[] otherArgs = new GenericOptionsParser(new Configuration(), args).getRemainingArgs();
		if (otherArgs.length < 3) {
			System.err.println("Usage: TrainDriver <in> <out> <train2ca>");
			System.exit(1);
		}

        Path inputPath = new Path(otherArgs[0]);
        Path outputPath = new Path (otherArgs[1]);
        URI train2ca = new URI(otherArgs[2] + "#MAP");

		Configuration trainJobConf = getConf();
//		trainJobConf.set("ID", "03");

		Job trainJob = Job.getInstance(trainJobConf, "Train Job");
		trainJob.addCacheFile(train2ca);
		trainJob.setJarByClass(TrainDriver.class);
		trainJob.setNumReduceTasks(1);
		trainJob.setMapperClass(TrainMapper.class);
		trainJob.setReducerClass(TrainReducer.class);
		trainJob.setMapOutputKeyClass(Text.class);
		trainJob.setMapOutputValueClass(IntWritable.class);
		trainJob.setOutputKeyClass(NullWritable.class);
		trainJob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(trainJob, inputPath);
		FileOutputFormat.setOutputPath(trainJob, outputPath);
		return trainJob.waitForCompletion(true) ? 0 : 1;
	}

}
