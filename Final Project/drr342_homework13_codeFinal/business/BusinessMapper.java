import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BusinessMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
	public void map(LongWritable offset, Text rowText, Context context) throws IOException, InterruptedException {
        String row = rowText.toString();
        if (row.contains("null")) return;
        row = row.replaceAll("\".+?\"", "");
        String[] tokens = row.split(",");
        if (tokens.length < 29) return;
        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
        Date date = null;
        try {
			date = sdf.parse(tokens[28]);
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
        String key = String.format("%d,%d,%d,%s", year, month, day, tokens[0]);
        context.write(new Text(key), new IntWritable(1));
    }
}
