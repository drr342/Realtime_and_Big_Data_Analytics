package soda;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.socrata.api.HttpLowLevel;
import com.socrata.api.Soda2Consumer;
import com.socrata.builders.SoqlQueryBuilder;
import com.socrata.exceptions.LongRunningQueryException;
import com.socrata.exceptions.SodaError;
import com.socrata.model.soql.SoqlQuery;
import com.sun.jersey.api.client.ClientResponse;

public class SodaTrains {

	public static void main(String[] args)
			throws LongRunningQueryException, SodaError, IOException, ClassNotFoundException, InterruptedException {

		Soda2Consumer consumer = Soda2Consumer.newConsumer("https://data.cityofchicago.org/", 
												"drr342@nyu.edu",
												"azTj8F7pHw39j8A", 
												"9lMNoOZCoQJCYbPOuUVQw7fXi");

		BufferedReader br = new BufferedReader(new FileReader (new File("Lstops.csv")));
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File("train2ca.csv")));
		br.readLine();
		Set<Integer> stations = new HashSet<>();
		while (true) {
			String line = br.readLine();
			if (line == null || line.isEmpty()) break;
			String[] tokens = line.split(",");
			int i = 5, id = 0;
			boolean flag = false;
			while (!flag) {
				try {
					id = Integer.parseInt(tokens[i]);
					flag = true;
				} catch (Exception e) {
					i++;
				}
			}
			if (stations.add(id)) {
				int size = tokens.length;
				String point = "POINT(" + 
						tokens[size-1].replaceAll("[\")( ]", "") + " " + 
						tokens[size-2].replaceAll("[\")( ]", "") + ")";
				String w = String.format("intersects(the_geom, '%s')", point);
				SoqlQuery query = new SoqlQueryBuilder()
						.addSelectPhrase("area_numbe")
						.setWhereClause(w)
						.build();
				ClientResponse response = consumer.query("igwz-8jzy", 
						HttpLowLevel.JSON_TYPE, 
						query);
				String out = response.getEntity(String.class);
				out = out.replaceAll("[^\\d]", "");
				if (out.isEmpty()) out = "null";
				bw.write(id + "," + out);
				bw.newLine();
			}
		}
		br.close();
		bw.close();
	}

}
