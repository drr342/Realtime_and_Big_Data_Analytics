import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class WordCount {

	public static void main(String[] args) throws IOException {
		String inputFile = "input.txt";
		BufferedReader reader = new BufferedReader(new FileReader(inputFile));
		String line;
		String[] queries = {"hackathon", "Dec", "Chicago", "Java"};
		int[] count = {0, 0, 0, 0};

		while ((line = reader.readLine()) != null) {
			for (int i = 0; i < queries.length; i++) {
				if (line.toLowerCase().contains(queries[i].toLowerCase()))
					count[i]++;
			}
		}

		String outputFile = "output.txt";
		BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
		for (int i = 0; i < queries.length; i++) {
			writer.write(queries[i] + " " + count[i]);
			writer.newLine();
			System.out.println(queries[i] + " " + count[i]);
		}

		reader.close();
		writer.close();
	}

}
