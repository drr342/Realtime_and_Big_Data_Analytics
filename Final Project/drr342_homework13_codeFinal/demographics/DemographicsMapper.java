import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DemographicsMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static Map<String, Integer> map = new HashMap<>();

    @Override
	protected void setup(Context context) throws IOException, InterruptedException {
        BufferedReader bf = new BufferedReader(new FileReader(new File("./MAP")));
        while (true) {
            String line = bf.readLine();
            if (line == null) break;
            String[] tokens = line.split(",");
            tokens[0] = tokens[0].replaceAll("\\.\\d+", "");
            map.put(tokens[0], Integer.parseInt(tokens[1]));
        }
		bf.close();
    }

    @Override
	public void map(LongWritable offset, Text rowText, Context context) throws IOException, InterruptedException {
        String row = rowText.toString();
        if (row.contains("GEO") || row.contains("Id")) return;
        String[] tokens = row.split(",");
        tokens[3] = tokens[3].replaceAll("\"Census Tract ", "");
        Integer value = map.get(tokens[3]);
        if (value != null) {
            String key = tokens[0] + "," + value;
            String out = Arrays.asList(tokens).subList(6, tokens.length).toString().replaceAll("[\\[\\] ]", "");
            context.write(new Text(key), new Text(out));
        }
    }

}
