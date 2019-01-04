import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TrainMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private static Map<String, Integer> train2ca = new HashMap<>();
	
    @Override
	protected void setup(Context context) throws IOException, InterruptedException {
        BufferedReader br = new BufferedReader(new FileReader(new File("./MAP")));
        while (true) {
            String line = br.readLine();
            if (line == null) break;
            String[] tokens = line.split(",");
            if (!tokens[1].equals("null")) {
            	train2ca.put(tokens[0], Integer.parseInt(tokens[1]));
            }
        }
		br.close();
    }

    @Override
	public void map(LongWritable offset, Text rowText, Context context) throws IOException, InterruptedException {
        String row = rowText.toString();
        String[] tokens = row.split(",");
        String id = tokens[0];
        if (!train2ca.containsKey(id)) return;
        int ca = train2ca.get(id);
        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
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
//        if (year < 2009 || year > 2016) return;
        String key =  String.format("%d,%d,%d,%s", year, month, day, ca);
//        int[] values = new int[8];
//        Arrays.fill(values, 0);
//        int index = calendar.get(Calendar.DAY_OF_WEEK);
        int value = Integer.parseInt(tokens[4]);
//        values[0] = value;
//        values[index] = value;
//        String val = Arrays.toString(values).replaceAll("[\\[\\] ]", "");
        context.write(new Text(key), new IntWritable(value));
    }
}
