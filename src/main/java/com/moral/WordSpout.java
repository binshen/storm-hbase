package com.moral;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * Created by bin.shen on 30/10/2016.
 */
public class WordSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private static final String[] MSGS = new String[]{
            "Storm", "HBase", "Integration", "example", "by ", "aloo", "in", "Aug",
    };

    private static final Random random = new Random();

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    public void nextTuple() {
        String word = MSGS[random.nextInt(8)];
        collector.emit(new Values(word));
    }
}