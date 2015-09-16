
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * This topology reads a file and counts the words in that file, then finds the top N words.
 */
public class TopWordFinderTopologyPartD {

    private static final int N = 10;

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
    TopNFinderBolt -> "top-n"


    ------------------------------------------------- */

        String spoutId = "spout";
        String splitId = "split";
        String countId = "count";
        String normalizeId = "normalize";
        String topn = "top-n";

        builder.setSpout(spoutId, new FileReaderSpout(args[0]), 5);
        builder.setBolt(splitId, new SplitSentenceBolt(), 8).shuffleGrouping(spoutId);
        builder.setBolt(normalizeId, new NormalizerBolt(), 12).fieldsGrouping(splitId, new Fields("word"));
        builder.setBolt(countId, new WordCountBolt(), 12).fieldsGrouping(normalizeId, new Fields("word"));
        builder.setBolt(topn, new TopNFinderBolt(N), 1).fieldsGrouping(countId, new Fields("word", "count"));


        config.setMaxTaskParallelism(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count", config, builder.createTopology());

        //wait for 2 minutes and then kill the job
        Thread.sleep(2 * 60 * 1000);

        cluster.shutdown();
    }
}
