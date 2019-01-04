import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.socrata.api.HttpLowLevel;
import com.socrata.api.Soda2Consumer;
import com.socrata.builders.SoqlQueryBuilder;
import com.socrata.exceptions.LongRunningQueryException;
import com.socrata.exceptions.SodaError;
import com.socrata.model.soql.SoqlQuery;
import com.sun.jersey.api.client.ClientResponse;

public class SodaMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    @Override
	public void map(LongWritable offset, Text rowText, Context context) throws IOException, InterruptedException {
        String line = rowText.toString();
        Pattern p = Pattern.compile("\\(-?\\d+(\\.\\d+)?, -?\\d+(\\.\\d+)?\\)");
        Matcher m;
        List<String> points = new ArrayList<>();
        m = p.matcher(line);
        while (m.find()) {
            String[] tokens = m.group().replaceAll("[() ]", "").split(",");
            String point = "POINT(" + tokens[1] + " " + tokens[0] + ")";
            points.add(point);
        }
        if (points.isEmpty()) return;
        Soda2Consumer consumer = Soda2Consumer.newConsumer("https://data.cityofchicago.org/",
                                        "drr342@nyu.edu",
                                        "azTj8F7pHw39j8A",
                                        "9lMNoOZCoQJCYbPOuUVQw7fXi");
        StringBuilder sb = new StringBuilder();
        for (String point: points) {
            String w = String.format("intersects(the_geom, '%s')", point);
            SoqlQuery query = new SoqlQueryBuilder()
                    .addSelectPhrase("area_numbe")
                    .setWhereClause(w)
                    .build();
            ClientResponse response = null;
            try {
                response = consumer.query("igwz-8jzy", HttpLowLevel.JSON_TYPE, query);
            } catch (LongRunningQueryException e) {
                e.printStackTrace();
                continue;
            } catch (SodaError e) {
                e.printStackTrace();
                continue;
            }
            String temp = response.getEntity(String.class).replaceAll("[^\\d]", "");
            if (temp.isEmpty()) continue;
            sb.append(temp);
            sb.append(",");
        }
        sb.append(line);
        context.write(NullWritable.get(), new Text(sb.toString()));
    }

}
