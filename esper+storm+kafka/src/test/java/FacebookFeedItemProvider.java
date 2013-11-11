/*
 * Author: cbedford
 * Date: 11/4/13
 * Time: 6:01 PM
 */


import com.restfb.Connection;
import com.restfb.DefaultFacebookClient;
import com.restfb.FacebookClient;
import com.restfb.Parameter;
import com.restfb.types.Post;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import  org.apache.commons.collections.buffer.CircularFifoBuffer;


public class FacebookFeedItemProvider implements IFeedItemProvider {
    public static final SimpleDateFormat GMT_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

    private static final int TIME_OVERLAP = 1000 * 60;  // one minute
    private static final int NUM_REMEMBERED_PREVIOUSLY_SEEN_ITEM_IDS = 1000;

    private final String queryString;
    private final FacebookClient facebookClient;
    private final ConcurrentLinkedQueue<String> itemQueue = new ConcurrentLinkedQueue<String>();

    private final CircularFifoBuffer prevSeenItemIds = new  CircularFifoBuffer(NUM_REMEMBERED_PREVIOUSLY_SEEN_ITEM_IDS);


    private volatile Date lastQueryTime = new Date();
    //private volatile Date lastQueryTime =    parseDate("2013-11-08T19:33:20-0800");


    public FacebookFeedItemProvider(String authToken, String queryString) {
        facebookClient = new DefaultFacebookClient(authToken);
        this.queryString = queryString;
    }

    public static void main(String[] args) {
        Date startDate =  parseDate("2013-11-08T19:33:20-0800");

        FacebookFeedItemProvider provider = new FacebookFeedItemProvider(args[0], "Rizal");
        Thread thread  =  new Thread(provider.getRunnableTask(), "facebookFeedItemProviderThread");
        thread.start();

        //System.out.println("Getting from queue");

        while (true) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            String item = provider.itemQueue.poll();
            if (item != null) {
                System.out.println("+++++++++++++ >>>: " + item);
            } else {
                //System.out.println("+++++++++++++ no queue item");
            }
        }
    }

    private static Date parseDate(String dateString) {
        Date startDate = null;
        try {
            startDate = GMT_DATE_FORMAT.parse(dateString);
            System.out.println("result of parse is " + getFormattedDate(startDate));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return startDate;
    }


    @Override
    public Runnable getRunnableTask() {
        return new Runnable() {
            @Override
            public void run() {
                while (true) {
                    // We set updatedLastQueryTime  to some time before the time we start our search so we don't
                    // miss any items posted while the search is being done. This means we can
                    // double process some items.  To avoid this we maintain a bounded queue of previously seen
                    // item ids.  If the number previously seen is more than the buffer bound we might double process,
                    // but for our demo we won't worry about this.
                    //
                    //System.out.println("starting query from: "  + getFormattedDate(lastQueryTime));
                    Date updatedLastQueryTime = new Date(  new Date().getTime() - TIME_OVERLAP );
                    //Date updatedLastQueryTime = new Date();
                    Connection<Post>  postStream = getPostStream();
                    List<Post> postList = postStream.getData();
                    if (postList.size() > 0) {
                        for (Post p : postList) {
                            //System.out.println("Post at : " + getFormattedDate(p.getCreatedTime()) + "\n" + p.getMessage() + " id = " + p.getId());
                            enqueueItemIfNotPreviouslySeen(p);
                        }
                    }
                    lastQueryTime = updatedLastQueryTime;

                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            private void enqueueItemIfNotPreviouslySeen(Post p) {
                String thisPostId = p.getId();
                boolean sawBefore = false;

                Iterator iter = prevSeenItemIds.iterator();
                while (iter.hasNext()) {
                    String seenId = (String) iter.next();
                    if (thisPostId.equals(seenId)) {
                        sawBefore = true;
                        break;
                    }
                }

                if (! sawBefore) {
                    prevSeenItemIds.add(thisPostId);
                    itemQueue.offer(p.getMessage());
                }  // on the other hand, if we saw it before then we do thing.. .just ignore
            }

        };
    }


    private Connection<Post> getPostStream() {
        return facebookClient.fetchConnection(
                "search",
                Post.class,
                Parameter.with("q", queryString),
                Parameter.with("since", lastQueryTime),
                Parameter.with("type", "post"));
    }


    @Override
    public Object getNextItemIfAvailable() {
        return itemQueue.poll();
    }

    private static String getFormattedDate(Date date) {
        String str;
        SimpleDateFormat sdf = GMT_DATE_FORMAT;
        return sdf.format(date);
    }

}