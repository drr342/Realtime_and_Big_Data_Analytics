import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* COLUMNS IN THE CRIMES DATASET
 * 00 ID - Int
 * 01 Case Number - String
 * 02 Date - Date
 * 03 Block - String
 * 04 IUCR - String
 * 05 Primary Type - String
 * 06 Description - String
 * 07 Location Description - String
 * 08 Arrest - Boolean
 * 09 Domestic - Boolean
 * 10 Beat - Int
 * 11 District - Int
 * 12 Ward - Int
 * 13 Community Area - Int
 * 14 FBI Code - String
 * 15 X Coordinate - Int
 * 16 Y Coordinate - Int
 * 17 Year - Int
 * 18 Updated On - Date
 * 19 Latitude - Double
 * 20 Longitude - Double
 * 21 Location  - String
*/

public class CrimeReducer extends Reducer<Text, Text, NullWritable, Text> {
	
//	private static String id;
	private static final int SIZE = 35;
	
//	@Override
//	protected void setup(Context context) {
//		id = context.getConfiguration().get("ID");
//	}

    @Override
	public void reduce(Text key, Iterable<Text> valuesText, Context context) throws IOException, InterruptedException {
    	int[] values = new int[SIZE];
    	Arrays.fill(values, 0);
		for (Text valueText : valuesText) {
			int[] tokens = Stream.of(valueText.toString().split(",")).mapToInt(Integer::parseInt).toArray();
			Arrays.setAll(values, i -> values[i] + tokens[i]);
		}
//		String out = id + "," + key + "," + Arrays.toString(values).replaceAll("[\\[\\] ]", "");
		String out = key + "," + Arrays.toString(values).replaceAll("[\\[\\] ]", "");
		context.write(NullWritable.get(), new Text(out));
    }

}
