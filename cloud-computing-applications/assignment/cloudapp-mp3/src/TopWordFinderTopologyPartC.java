
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * This topology reads a file, splits the senteces into words, normalizes the words such that all words are
 * lower case and common words are removed, and then count the number of words.
 */
public class TopWordFinderTopologyPartC {

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
    NormalizerBolt -> "normalize"
    ------------------------------------------------- */
        String spoutId = "spout";
        String splitId = "split";
        String countId = "count";
        String normalizeId = "normalize";

        builder.setSpout(spoutId, new FileReaderSpout(args[0]), 5);
        builder.setBolt(splitId, new SplitSentenceBolt(), 8).shuffleGrouping(spoutId);
        builder.setBolt(normalizeId, new NormalizerBolt(), 12).fieldsGrouping(splitId, new Fields("word"));
        builder.setBolt(countId, new WordCountBolt(), 12).fieldsGrouping(normalizeId, new Fields("word"));

        config.setMaxTaskParallelism(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count", config, builder.createTopology());

        //wait for 2 minutes then kill the job
        Thread.sleep(2 * 60 * 1000);

        cluster.shutdown();
    }
}
