import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DemographicsReducer extends Reducer<Text, Text, NullWritable, Text> {
	
//	private static String id;
//	
//	@Override
//	protected void setup(Context context) {
//		id = context.getConfiguration().get("ID");
//	}

    @Override
	public void reduce(Text key, Iterable<Text> rowsText, Context context) throws IOException, InterruptedException {
        int size = 118;
        int[] values = new int[size];
        Arrays.fill(values, 0);
        for (Text rowText : rowsText) {
            String row = rowText.toString();
            String[] tokens = row.split(",");
            for (int i = 0; i < values.length; i++) {
                values[i] += Integer.parseInt(tokens[i]);
            }
        }
        String out = Arrays.toString(values).replaceAll("[\\[\\] ]", "");
        context.write(NullWritable.get(), new Text(/*id + "," + */key + "," + out));
    }

}
