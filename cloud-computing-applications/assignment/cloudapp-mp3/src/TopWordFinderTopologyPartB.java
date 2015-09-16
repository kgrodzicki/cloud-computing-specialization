
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * This topology reads a file and counts the words in that file
 */
public class TopWordFinderTopologyPartB {

    public static void main(String[] args) throws Exception {


        TopologyBuilder builder = new TopologyBuilder();

        Config config = new Config();
        config.setDebug(true);


    /*
    ----------------------TODO-----------------------
    Task: wire up the topology

    NOTE:make sure when connecting components together, using the functions setBolt(name,…) and setSpout(name,…),
    you use the following names for each component:
    FileReaderSpout -> "spout"
    SplitSentenceBolt -> "split"
    WordCountBolt -> "count"
    ------------------------------------------------- */

        String spoutId = "spout";
        String splitId = "split";
        String countId = "count";

        builder.setSpout(spoutId, new FileReaderSpout(args[0]), 5);
        builder.setBolt(splitId, new SplitSentenceBolt(), 8).shuffleGrouping(spoutId);
        builder.setBolt(countId, new WordCountBolt(), 12).fieldsGrouping(splitId, new Fields("word"));

        config.setMaxTaskParallelism(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count", config, builder.createTopology());

        //wait for 2 minutes and then kill the job
        Thread.sleep(2 * 60 * 1000);

        cluster.shutdown();
    }
}
