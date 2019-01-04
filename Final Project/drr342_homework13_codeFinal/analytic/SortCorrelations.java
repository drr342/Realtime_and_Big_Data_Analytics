import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class SortCorrelations {

	private static List<String> crimes = new ArrayList<>();
	private static final int CRIME_SIZE = 35;
	private static final int CA_SIZE = 78;

	private static void populateCrimes () throws IOException {
		BufferedReader br = new BufferedReader(new FileReader (new File("crimeTypes.csv")));
		while (true) {
			String crime = br.readLine();
			if (crime == null || crime.isEmpty()) break;
			crimes.add(crime);
		}
		br.close();
	}

	private static class Register implements Comparable<Register> {

		private String Factor1, Factor2;
		private int count, ca;
		private double corr;

		private Register(String factor, String count, String corr, int index, int ca) {
			this.Factor1 = factor;
			this.Factor2 = crimes.get(index);
			this.count = Integer.parseInt(count);
			this.ca = ca;
			this.corr = Double.parseDouble(corr);

		}

		@Override
		public int compareTo(Register other) {
			return (int) Math.signum(Math.abs(other.corr) - Math.abs(this.corr));
		}

		@Override
		public String toString () {
			String out = "";
			out += this.Factor1 + ",";
			out += this.Factor2 + ",";
			out += this.count + ",";
			out += this.ca + ",";
			out += this.corr;
			return out;
		}

	}

	public static void main(String[] args) throws IOException {
		populateCrimes();
		Set<Register> entries = new TreeSet<>();
		for (int j = 0; j < CA_SIZE; j++) {
			BufferedReader br = new BufferedReader(new FileReader (new File("temp/" + (j+1) + ".csv")));
			while (true) {
				String line = br.readLine();
				if (line == null || line.isEmpty()) break;
				String[] tokens = line.split(",");
				for (int i = 0; i < CRIME_SIZE; i++) {
					if (tokens[i + 2].equals("NaN")) continue;
					Register entry = new Register(tokens[0], tokens[1], tokens[i + 2], i, j);
					if (Math.abs(entry.corr) < 0.8) continue;
					entries.add(entry);
				}
			}
			br.close();
		}
		BufferedWriter bw = new BufferedWriter(new FileWriter (new File("../results.csv")));
		bw.write("FACTOR,CRIME_TYPE,COUNT,COMMUNITY_AREA,CORRELATION");
		bw.newLine();
		Iterator<Register> it = entries.iterator();
		while(it.hasNext()) {
			bw.write(it.next().toString());
			bw.newLine();
		}
		bw.close();
	}

}
