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
    private final String[] searchTerms;
    private final ConcurrentLinkedQueue<String> itemQueue = new ConcurrentLinkedQueue<String>();

    public class TwitterListener implements StatusListener {
        @Override
        public void onStatus(Status status) {
            String text = status.getText();
            if(status.isRetweet()){
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

    public static void main(String[] args) throws TwitterException, IOException {
        TwitterFeedItemProvider provider = new TwitterFeedItemProvider("#reviews");
        Thread thread = new Thread(provider.getRunnableTask(), "twitterFeedItemProviderThread");
        thread.start();

        while (true) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            String item = (String) provider.getNextItemIfAvailable(); //itemQueue.poll();
            if (item != null) {
                System.out.println("+++++++++++++ >>>: " + item);
            } else {
                //System.out.println("+++++++++++++ no queue item");
            }
        }
    }

    TwitterFeedItemProvider(List<String> searchTermsList) {
        this.searchTerms = searchTermsList.toArray(new String[searchTermsList.size()]);
    }

    TwitterFeedItemProvider(String... terms) {
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
        builder.setOAuthConsumerKey(System.getenv("setOAuthConsumerKey"));
        builder.setOAuthConsumerSecret(System.getenv("setOAuthConsumerSecret"));
        builder.setOAuthAccessToken(System.getenv("setOAuthAccessToken"));
        builder.setOAuthAccessTokenSecret(System.getenv("setOAuthAccessTokenSecret"));

        Configuration conf = builder.build();

        twitterStream = new TwitterStreamFactory(conf).getInstance();
        return twitterStream;
    }

    @Override
    public Object getNextItemIfAvailable() {
        return itemQueue.poll();
    }
}
