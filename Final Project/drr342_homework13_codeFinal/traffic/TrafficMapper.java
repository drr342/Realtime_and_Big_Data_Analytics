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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TrafficMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private static Map<String, String> traffic2ca = new HashMap<>();
	
    @Override
	protected void setup(Context context) throws IOException, InterruptedException {
        BufferedReader br = new BufferedReader(new FileReader(new File("./MAP")));
        while (true) {
            String line = br.readLine();
            if (line == null) break;
            String[] tokens = line.split(",");
            if (!tokens[1].equals("null")) {
            	traffic2ca.put(tokens[0], tokens[1]);
            }
        }
		br.close();
    }

    @Override
	public void map(LongWritable offset, Text rowText, Context context) throws IOException, InterruptedException {
        String row = rowText.toString();
        String[] tokens = row.split(",");
        String id = tokens[1];
        if (!traffic2ca.containsKey(id)) return;
        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
        Date date = null;
        try {
			date = sdf.parse(tokens[0]);
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
        Integer bus = null, speed = null;
        try {
        	bus = Integer.parseInt(tokens[2]);
        	speed = Integer.parseInt(tokens[4]);
        } catch (Exception e) {
        	e.printStackTrace();
        	return;
        }
        if (bus == 0 || speed == -1) return;
        String[] cas = traffic2ca.get(id).split(" ");
        for (String ca : cas) {
            String key =  String.format("%d,%d,%d,%s", year, month, day, ca);
            context.write(new Text(key), new Text(bus + "," + speed));
        }
    }
}
