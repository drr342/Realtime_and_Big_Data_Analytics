import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

public class GenerateQuery {
	
	private static final int ca = 77;
	
	private static final String business = 
			"\t\tSUM(chicago_business.business_total) as business_total,\n";
	private static final String demo = 
			"\t\tSUM(chicago_demo.demo_HD01_VD01) as demo_HD01_VD01,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD02) as demo_HD01_VD02,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD03) as demo_HD01_VD03,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD04) as demo_HD01_VD04,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD05) as demo_HD01_VD05,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD06) as demo_HD01_VD06,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD07) as demo_HD01_VD07,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD08) as demo_HD01_VD08,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD09) as demo_HD01_VD09,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD10) as demo_HD01_VD10,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD11) as demo_HD01_VD11,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD12) as demo_HD01_VD12,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD13) as demo_HD01_VD13,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD14) as demo_HD01_VD14,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD15) as demo_HD01_VD15,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD16) as demo_HD01_VD16,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD17) as demo_HD01_VD17,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD18) as demo_HD01_VD18,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD19) as demo_HD01_VD19,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD20) as demo_HD01_VD20,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD21) as demo_HD01_VD21,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD22) as demo_HD01_VD22,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD23) as demo_HD01_VD23,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD24) as demo_HD01_VD24,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD25) as demo_HD01_VD25,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD26) as demo_HD01_VD26,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD27) as demo_HD01_VD27,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD28) as demo_HD01_VD28,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD29) as demo_HD01_VD29,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD30) as demo_HD01_VD30,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD31) as demo_HD01_VD31,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD32) as demo_HD01_VD32,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD33) as demo_HD01_VD33,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD34) as demo_HD01_VD34,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD35) as demo_HD01_VD35,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD36) as demo_HD01_VD36,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD37) as demo_HD01_VD37,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD38) as demo_HD01_VD38,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD39) as demo_HD01_VD39,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD40) as demo_HD01_VD40,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD41) as demo_HD01_VD41,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD42) as demo_HD01_VD42,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD43) as demo_HD01_VD43,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD44) as demo_HD01_VD44,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD45) as demo_HD01_VD45,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD46) as demo_HD01_VD46,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD47) as demo_HD01_VD47,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD48) as demo_HD01_VD48,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD49) as demo_HD01_VD49,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD50) as demo_HD01_VD50,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD51) as demo_HD01_VD51,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD52) as demo_HD01_VD52,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD53) as demo_HD01_VD53,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD54) as demo_HD01_VD54,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD55) as demo_HD01_VD55,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD56) as demo_HD01_VD56,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD57) as demo_HD01_VD57,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD58) as demo_HD01_VD58,\n"
					+"\t\tSUM(chicago_demo.demo_HD01_VD59) as demo_HD01_VD59,\n";
	private static final String crime = 
			"\t\tSUM(chicago_crime.crime_arson) as crime_arson,\n"
					+"\t\tSUM(chicago_crime.crime_assault) as crime_assault,\n"
					+"\t\tSUM(chicago_crime.crime_battery) as crime_battery,\n"
					+"\t\tSUM(chicago_crime.crime_burglary) as crime_burglary,\n"
					+"\t\tSUM(chicago_crime.crime_ccl_violation) as crime_ccl_violation,\n"
					+"\t\tSUM(chicago_crime.crime_criminal_damage) as crime_criminal_damage,\n"
					+"\t\tSUM(chicago_crime.crime_criminal_trespass) as crime_criminal_trespass,\n"
					+"\t\tSUM(chicago_crime.crime_crim_sexual_assault) as crime_crim_sexual_assault,\n"
					+"\t\tSUM(chicago_crime.crime_deceptive_practice) as crime_deceptive_practice,\n"
					+"\t\tSUM(chicago_crime.crime_domestic_violence) as crime_domestic_violence,\n"
					+"\t\tSUM(chicago_crime.crime_gambling) as crime_gambling,\n"
					+"\t\tSUM(chicago_crime.crime_homicide) as crime_homicide,\n"
					+"\t\tSUM(chicago_crime.crime_human_trafficking) as crime_human_trafficking,\n"
					+"\t\tSUM(chicago_crime.crime_interference) as crime_interference,\n"
					+"\t\tSUM(chicago_crime.crime_intimidation) as crime_intimidation,\n"
					+"\t\tSUM(chicago_crime.crime_kidnapping) as crime_kidnapping,\n"
					+"\t\tSUM(chicago_crime.crime_liquor) as crime_liquor,\n"
					+"\t\tSUM(chicago_crime.crime_motor_vehicle_theft) as crime_motor_vehicle_theft,\n"
					+"\t\tSUM(chicago_crime.crime_narcotics) as crime_narcotics,\n"
					+"\t\tSUM(chicago_crime.crime_non_criminal) as crime_non_criminal,\n"
					+"\t\tSUM(chicago_crime.crime_non_criminal_ss) as crime_non_criminal_ss,\n"
					+"\t\tSUM(chicago_crime.crime_obscenity) as crime_obscenity,\n"
					+"\t\tSUM(chicago_crime.crime_offense_children) as crime_offense_children,\n"
					+"\t\tSUM(chicago_crime.crime_other_narcotic) as crime_other_narcotic,\n"
					+"\t\tSUM(chicago_crime.crime_other_offense) as crime_other_offense,\n"
					+"\t\tSUM(chicago_crime.crime_prostitution) as crime_prostitution,\n"
					+"\t\tSUM(chicago_crime.crime_public_indecency) as crime_public_indecency,\n"
					+"\t\tSUM(chicago_crime.crime_peace_violation) as crime_peace_violation,\n"
					+"\t\tSUM(chicago_crime.crime_ritualism) as crime_ritualism,\n"
					+"\t\tSUM(chicago_crime.crime_robbery) as crime_robbery,\n"
					+"\t\tSUM(chicago_crime.crime_sex_offense) as crime_sex_offense,\n"
					+"\t\tSUM(chicago_crime.crime_stalking) as crime_stalking,\n"
					+"\t\tSUM(chicago_crime.crime_theft) as crime_theft,\n"
					+"\t\tSUM(chicago_crime.crime_weapons) as crime_weapons,\n"
					+"\t\tSUM(chicago_crime.crime_total) as crime_total\n";
	private static final String traffic = 
	        "\t\tSUM(chicago_traffic.traffic_bus) as traffic_bus,\n"
	        		+"\t\tAVG(chicago_traffic.traffic_speed_avg) as traffic_speed_avg,\n"
	        		+"\t\tMIN(chicago_traffic.traffic_speed_min) as traffic_speed_min,\n"
	        		+"\t\tMAX(chicago_traffic.traffic_speed_max) as traffic_speed_max,\n";
	private static final String train =
			"\t\tSUM(chicago_train.train_total) as train_total,\n";

	public static void main(String[] args) throws IOException {
		Set<String> featuresSet = new LinkedHashSet<>();
		Set<String> crimesSet = new LinkedHashSet<>();
		BufferedReader br = new BufferedReader(new FileReader(new File("temp")));
		while (true) {
			String line = br.readLine();
			if (line == null) break;
			line = line.split(",")[0];
			if (line.endsWith("_year") || 
					line.endsWith("_month") || 
					line.endsWith("_day") || 
					line.endsWith("_ca") ||
					line.contains("_hd02_")) continue;
			if (line.startsWith("crime_"))
				crimesSet.add(line);
			else
				featuresSet.add(line);
		}
		br.close();
		String[] crimes = new String[crimesSet.size()]; 
		crimesSet.toArray(crimes);
		String[] features = new String[featuresSet.size()]; 
		featuresSet.toArray(features);
		StringBuilder sb = new StringBuilder();
		sb.append("USE ${VAR:user};\n\n");
		for (int k = 0; k <= ca; k++) {
			for (int j = 0; j < features.length; j++) {
				String type = (j == 0) ? "OVERWRITE" : "INTO";
				sb.append(String.format("INSERT %s TABLE correlation_%02d\n", type, k));
				sb.append("SELECT\n");
				sb.append(String.format("\t'%s',n", features[j]));
				for (int i = 0; i < crimes.length; i++) {
					sb.append(String.format(",\n\t(ex%dy%d-ex%d*ey%d)/(sx%d*sy%d)"
							, j, i, j, i, j, i));
				}
				sb.append("\nFROM(\n");
				sb.append("\tSELECT\n");
				sb.append(String.format("\t\tAVG(%s) AS ex%d,\n", features[j], j));
				sb.append(String.format("\t\tSTDDEV(%s) AS sx%d,\n", features[j], j));
				for (int i = 0; i < crimes.length; i++) {
					sb.append(String.format("\t\tAVG(%s) AS ey%d,\n", crimes[i], i));
					sb.append(String.format("\t\tAVG(%s*%s) AS ex%dy%d,\n", features[j], crimes[i], j, i));
					sb.append(String.format("\t\tSTDDEV(%s) AS sy%d,\n", crimes[i], i));
				}
				sb.append("\tcount(*) AS n\n");
				sb.append("FROM (\n\n");
				String table = "chicago_" + features[j].split("_")[0];
				sb.append(join(k, table));
				sb.append("\n\t) AS joined\n");
				sb.append(") AS corr;\n");
			}
		}

//		sb.append("SELECT * FROM correlation;");
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File("correlation.sql")));
		bw.write(sb.toString());
		bw.close();
	}
	
	private static String join (int k, String table) {
		StringBuilder sb = new StringBuilder();
		sb.append("\tSELECT\n");
		switch (table) {
		case "chicago_business":
			sb.append(business);
			break;
		case "chicago_demo":
			sb.append(demo);
			break;
		case "chicago_traffic":
			sb.append(traffic);
			break;
		case "chicago_train":
			sb.append(train);
			break;
		}
		sb.append(crime);
		sb.append("\tFROM " + table + " JOIN chicago_crime\n");
		sb.append("\tON (\n");
		if (k == 0) {
			switch (table) {
			case "chicago_demo":
				sb.append(String.format("\t\tchicago_crime.crime_year = %s.%s_year AND\n", table, table.split("_")[1]));
				sb.append(String.format("\t\tchicago_crime.crime_ca = %s.%s_ca\n", table, table.split("_")[1]));
				sb.append("\t)\n\tGROUP BY\n");
				sb.append("\t\tchicago_crime.crime_year,\n");
				sb.append("\t\tchicago_crime.crime_ca\n");
				break;
			default:
				sb.append(String.format("\t\tchicago_crime.crime_year = %s.%s_year AND\n", table, table.split("_")[1]));
				sb.append(String.format("\t\tchicago_crime.crime_month = %s.%s_month AND\n", table, table.split("_")[1]));
				sb.append(String.format("\t\tchicago_crime.crime_ca = %s.%s_ca\n", table, table.split("_")[1]));
				sb.append("\t)\n\tGROUP BY\n");
				sb.append("\t\tchicago_crime.crime_year,\n"); 
				sb.append("\t\tchicago_crime.crime_month,\n");
				sb.append("\t\tchicago_crime.crime_ca\n");
			}
		} else {
			switch (table) {
			case "chicago_demo":
				sb.append(String.format("\t\tchicago_crime.crime_year = %s.%s_year\n", table, table.split("_")[1]));
				sb.append("\t)\n\tWHERE\n");
				sb.append("\t\tchicago_crime.crime_ca = " + k + "\n");
				sb.append("\tGROUP BY\n");
				sb.append("\t\tchicago_crime.crime_year\n"); 
				break;
			default:
				sb.append(String.format("\t\tchicago_crime.crime_year = %s.%s_year AND\n", table, table.split("_")[1]));
				sb.append(String.format("\t\tchicago_crime.crime_month = %s.%s_month\n", table, table.split("_")[1]));
				sb.append("\t)\n\tWHERE\n");
				sb.append("\t\tchicago_crime.crime_ca = " + k + "\n");
				sb.append("\tGROUP BY\n");
				sb.append("\t\tchicago_crime.crime_year,\n"); 
				sb.append("\t\tchicago_crime.crime_month\n");
			}
		}
		return sb.toString();
	}

}
