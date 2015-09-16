import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * This topology counts the words from sentences emmited from a random sentence spout.
 */
public class TopWordFinderTopologyPartA {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        Config config = new Config();
        config.setDebug(true);

    /*
    ----------------------TODO-----------------------
    Task: wire up the topology

    NOTE:make sure when connecting components together, using the functions setBolt(name,…) and setSpout(name,…),
    you use the following names for each component:

    RandomSentanceSpout -> "spout"
    SplitSentenceBolt -> "split"
    WordCountBolt -> "count"


    ------------------------------------------------- */
        String spoutId = "spout";
        String splitId = "split";
        String countId = "count";
        
        builder.setSpout(spoutId, new RandomSentenceSpout(), 5);
        builder.setBolt(splitId, new SplitSentenceBolt(), 8).shuffleGrouping(spoutId);
        builder.setBolt(countId, new WordCountBolt(), 12).fieldsGrouping(splitId, new Fields("word"));

        config.setMaxTaskParallelism(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count", config, builder.createTopology());

        //wait for 60 seconds and then kill the topology
        Thread.sleep(60 * 1000);

        cluster.shutdown();
    }
}
