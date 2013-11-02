/*
 * Author: cbedford
 * Date: 10/31/13
 * Time: 2:16 PM
 */


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

public class SentenceSpout extends BaseRichSpout {
    private transient SpoutOutputCollector collector;

    private static String[] sentences;
    private int sentenceIndex = 0;

    SentenceSpout(String[] sentences) {
        this.sentences = sentences;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (sentenceIndex < sentences.length) {
            String sentence = sentences[sentenceIndex];
            System.out.println("+++++++++++++++++ output sentence: " + sentence);
            collector.emit(new Values(sentence));
            sentenceIndex++;
        }
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

}
