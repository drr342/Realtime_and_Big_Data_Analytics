import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

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

public class CrimeMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static Map<String, Integer> types = new HashMap<>();
	private static int size;
	
	public static int getSize() {
		return size;
	}
	
	@Override
	protected void setup(Context context) throws IOException {
        BufferedReader bf = new BufferedReader(new FileReader(new File("./MAP")));
        int index = 0;
        while (true) {
            String line = bf.readLine();
            if (line == null) break;
            types.put(line, index++);
        }
        types.put("TOTAL", index);
        size = types.size();
		bf.close();
	}

    @Override
	public void map(LongWritable offset, Text rowText, Context context) throws IOException, InterruptedException {
        String row = rowText.toString();
        if (row.contains("Year")) return;
        row = row.replaceAll("\".+?\"", "");
        String[] tokens = row.split(",");
        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
        Date date = null;
        try {
        	date = sdf.parse(tokens[2]);
		} catch (ParseException e) {
			e.printStackTrace();
			return;
		}
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(date);
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        int day = calendar.get(Calendar.DAY_OF_MONTH);
        String ca = tokens[13];
        if (ca.isEmpty() || ca.equals("0")) return;
        String key = String.format("%d,%d,%d,%s", year, month, day, ca);
        String type = tokens[5].replaceAll("[^\\w]", " ").replaceAll("\\s+", " ").trim();
        Integer index = types.get(type);
        if (index == null) {
        	index = types.get("OTHER OFFENSE");
        }
        int[] values = new int[size];
        Arrays.fill(values, 0);
        values[index] = 1;
        values[size-1] = 1;
        String out = Arrays.toString(values).replaceAll("[\\[\\] ]", "");
        context.write(new Text(key), new Text(out));
    }

}
