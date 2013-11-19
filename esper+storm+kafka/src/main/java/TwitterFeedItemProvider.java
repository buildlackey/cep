/*
 * Author: cbedford
 * Date: 11/10/13
 * Time: 1:19 PM
 */


import twitter4j.*;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;


public class TwitterFeedItemProvider implements IFeedItemProvider {
    private final ConcurrentLinkedQueue<String> itemQueue = new ConcurrentLinkedQueue<String>();

    private final String oAuthConsumerKey;
    private final String oAuthConsumerSecret;
    private final String oAuthAccessToken;
    private final String oAuthAccessTokenSecret;
    private final String[] searchTerms;


    public class TwitterListener implements StatusListener {
        @Override
        public void onStatus(Status status) {
            String text = status.getText();
            if (status.isRetweet()) {
                text = status.getRetweetedStatus().getText();
            }
            itemQueue.offer(text);
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

        @Override
        public void onException(Exception ex) {
            ex.printStackTrace();
        }
    }

    /*

    TwitterFeedItemProvider(List<String> searchTermsList) {
        this.searchTerms = searchTermsList.toArray(new String[searchTermsList.size()]);
    }
     */

    TwitterFeedItemProvider(final String oAuthConsumerKey,
                            final String oAuthConsumerSecret,
                            final String oAuthAccessToken,
                            final String oAuthAccessTokenSecret,
                            String... terms) {
        this.oAuthConsumerKey = oAuthConsumerKey;
        this.oAuthConsumerSecret = oAuthConsumerSecret;
        this.oAuthAccessToken = oAuthAccessToken;
        this.oAuthAccessTokenSecret = oAuthAccessTokenSecret;

        this.searchTerms = terms;
    }

    @Override
    public Runnable getRunnableTask() {
        return new Runnable() {
            @Override
            public void run() {
                TwitterStream twitterStream = getTwitterStream();
                twitterStream.addListener(new TwitterListener());
                long[] followArray = new long[0];
                twitterStream.filter(new FilterQuery(0, followArray, searchTerms));
            }
        };
    }

    private TwitterStream getTwitterStream() {
        TwitterStream twitterStream;
        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.setOAuthConsumerKey(oAuthConsumerKey);
        builder.setOAuthConsumerSecret(oAuthConsumerSecret);
        builder.setOAuthAccessToken(oAuthAccessToken);
        builder.setOAuthAccessTokenSecret(oAuthAccessTokenSecret);

        Configuration conf = builder.build();

        twitterStream = new TwitterStreamFactory(conf).getInstance();
        return twitterStream;
    }

    @Override
    public Object getNextItemIfAvailable() {
        return itemQueue.poll();
    }
}
