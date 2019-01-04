import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public final class StreamToFile {

	public static String[] keywords = { "NYU", "Trump" };
	public static String consumerKey = "";
	public static String consumerSecret = "";
	public static String accessToken = "";
	public static String accessTokenSecret = "";

	@SuppressWarnings("resource")
	public static void main(String[] args) throws TwitterException, IOException {
		BufferedWriter br = new BufferedWriter(
				new OutputStreamWriter(new FileOutputStream("sampleStreamOutput.txt"), StandardCharsets.UTF_8));
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
				.setOAuthConsumerSecret(consumerSecret)
				.setOAuthAccessToken(accessToken)
				.setOAuthAccessTokenSecret(accessTokenSecret);

		TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance()
				.addListener(new StatusListener() {
					@Override
					public void onStatus(Status status) {
						try {
							br.write(status.getCreatedAt() + " - @" + status.getUser().getScreenName() + " - "
									+ status.getText());
							br.newLine();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void onException(Exception ex) {
						ex.printStackTrace();
					}

					@Override
					public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
					}

					@Override
					public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
					}

					@Override
					public void onScrubGeo(long userId, long upToStatusId) {
					}

					@Override
					public void onStallWarning(StallWarning warning) {
					}
				});

		// twitterStream.sample("en");

		FilterQuery tweetFilterQuery = new FilterQuery();
		tweetFilterQuery.track(keywords);
		// tweetFilterQuery.locations(new double[][]{new
		// double[]{-126.562500,30.448674},
		// new double[]{-61.171875,44.087585
		// }});
		tweetFilterQuery.language(new String[] { "en" });
		twitterStream.filter(tweetFilterQuery);
	}

}
