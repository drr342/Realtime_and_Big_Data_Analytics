import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: PageRank <path> <iterations>");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator", " ");
		FileSystem fs = FileSystem.get(conf);		

		int i = 0;
		int n = Integer.parseInt(args[1]);
		while (i < n) {
			Job job = new Job(conf);
			job.setJobName("PageRank(iteration" + i + ")");
			job.setNumReduceTasks(1);

			job.setJarByClass(PageRank.class);
			job.setMapperClass(PageRankMapper.class);
			job.setReducerClass(PageRankReducer.class);
			
			Path in = new Path(args[0] + "data" + i + "/");
   			Path out = new Path(args[0] + "data" + (i + 1));
			if (fs.exists(out)) fs.delete(out, true);
				
			FileInputFormat.addInputPath(job, in);
			FileOutputFormat.setOutputPath(job, out);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.waitForCompletion(true);
			i++;			
		}
		System.exit(0);
	}
}
