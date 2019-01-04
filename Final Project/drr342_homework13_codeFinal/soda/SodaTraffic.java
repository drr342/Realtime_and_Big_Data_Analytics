package soda;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import com.socrata.api.HttpLowLevel;
import com.socrata.api.Soda2Consumer;
import com.socrata.builders.SoqlQueryBuilder;
import com.socrata.exceptions.LongRunningQueryException;
import com.socrata.exceptions.SodaError;
import com.socrata.model.soql.SoqlQuery;
import com.sun.jersey.api.client.ClientResponse;

public class SodaTraffic {

	public static void main(String[] args)
			throws LongRunningQueryException, SodaError, IOException, ClassNotFoundException, InterruptedException {

		Soda2Consumer consumer = Soda2Consumer.newConsumer("https://data.cityofchicago.org/", 
												"drr342@nyu.edu",
												"azTj8F7pHw39j8A", 
												"9lMNoOZCoQJCYbPOuUVQw7fXi");

		BufferedReader br = new BufferedReader(new FileReader (new File("traffic.csv")));
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File("traffic2ca.csv")));
		br.readLine();
		while (true) {
			String line = br.readLine();
			if (line == null || line.isEmpty()) break;
			String[] tokens = line.split(",");
			int id = Integer.parseInt(tokens[0]);
			double startX = Double.parseDouble(tokens[8]);
			double startY = Double.parseDouble(tokens[9]);
			double endX = Double.parseDouble(tokens[10]);
			double endY = Double.parseDouble(tokens[11]);
			String segment = String.format("LINESTRING (%f %f, %f %f)", startX, startY, endX, endY);
			String w = String.format("intersects(the_geom, '%s')", segment);
			SoqlQuery query = new SoqlQueryBuilder()
					.addSelectPhrase("area_numbe")
					.setWhereClause(w)
					.build();
			ClientResponse response = consumer.query("igwz-8jzy", 
					HttpLowLevel.JSON_TYPE, 
					query);
			String out = response.getEntity(String.class);
			out = out.replaceAll("[^\\d]+", " ").trim();
			if (out.isEmpty()) out = "null";
			bw.write(String.format("%s,%s", id, out));
			bw.newLine();
		}
		br.close();
		bw.close();
	}

}
