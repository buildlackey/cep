/*
 * Author: cbedford
 * Date: 11/4/13
 * Time: 6:01 PM
 */


import java.util.concurrent.ConcurrentLinkedQueue;

class TestFeedItemProvider implements IFeedItemProvider {
    ConcurrentLinkedQueue<String> itemQueue = new ConcurrentLinkedQueue<String>();

    @Override
    public Runnable getRunnableTask() {
        return new Runnable() {
            @Override
            public void run() {
                for (String sentence : ExternalFeedToKafkaAdapterSpoutTest.sentences) {
                    itemQueue.offer(sentence);
                }
                try {
                    Thread.sleep(1000 * 100);
                } catch (InterruptedException e) {
                    e.printStackTrace();   // do something more meaningful here?
                }
            }
        };
    }

    @Override
    public Object getNextItemIfAvailable() {
        return itemQueue.poll();
    }
}