package com.moral;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.testing.TestWordCounter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * Created by bin.shen on 30/10/2016.
 * http://www.aloo.me/2016/08/14/%E5%9C%A8Storm%E4%B8%AD%E6%93%8D%E4%BD%9CHBase/
 */
public class PersistentWordCount {
    private static final String WORD_SPOUT = "WORD_SPOUT";
    private static final String COUNT_BOLT = "COUNT_BOLT";
    private static final String HBASE_BOLT = "HBASE_BOLT";

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "E:/BaiduYunDownload");

        Config config = new Config();

        WordSpout spout = new WordSpout();
        WordCounter bolt = new WordCounter();
        MyHBaseBolt hbase = new MyHBaseBolt();

        // wordSpout ==> countBolt ==> HBaseBolt
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(WORD_SPOUT, spout, 1);
        builder.setBolt(COUNT_BOLT, bolt, 1).shuffleGrouping(WORD_SPOUT);
        builder.setBolt(HBASE_BOLT, hbase, 10).fieldsGrouping(COUNT_BOLT, new Fields("word"));

        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word", config, builder.createTopology());
            Thread.sleep(10000);
            cluster.killTopology("word");
            cluster.shutdown();
            System.exit(0);
        } else {
            config.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        }
    }
}