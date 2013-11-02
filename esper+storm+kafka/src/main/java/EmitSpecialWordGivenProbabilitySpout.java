import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

import java.util.Map;

import static backtype.storm.utils.Utils.tuple;

public class EmitSpecialWordGivenProbabilitySpout extends BaseRichSpout
{
    private static final long serialVersionUID = 1L;

    private final String specialWord;
    private final int sleepMillisecsAfterEmission;
    private final double defaultWordEmissionProbability;
    private transient SpoutOutputCollector collector;

    public EmitSpecialWordGivenProbabilitySpout(String specialWord, double emissionProbability, int timesPerSec) {
        if (emissionProbability < 0 || emissionProbability > 1.0 ) {
            throw new IllegalArgumentException("Probability must be between 0 and 1.0");
        }
        this.specialWord =  specialWord;
        this.defaultWordEmissionProbability =  1.0 - emissionProbability;
        this.sleepMillisecsAfterEmission  = 1000 / timesPerSec;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("word"));

    }

    @Override
    public void nextTuple()
    {
        String wordToEmit = "default";
        if (Math.random() > defaultWordEmissionProbability) {
            wordToEmit = specialWord  + Math.floor ((Math.random() * 100));
        }
        collector.emit( tuple(wordToEmit));
        System.out.println("+++++emitted: " + wordToEmit);

        try {
            Thread.sleep(sleepMillisecsAfterEmission);
        } catch (InterruptedException e) {
            System.out.println("Fatal error");
            System.exit(-1);
        }
    }

    @Override
    public void open(@SuppressWarnings("rawtypes") Map conf,
                     TopologyContext context,
                     SpoutOutputCollector collector)
    {
        this.collector = collector;
    }

    @Override
    public void close() {}

    @Override
    public void ack(Object msgId) {}

    @Override
    public void fail(Object msgId) {}
}
