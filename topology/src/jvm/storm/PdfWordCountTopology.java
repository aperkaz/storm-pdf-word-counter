package storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import storm.spouts.PdfSpoolingSpout;

import storm.bolts.ParseWordBolt;
import storm.bolts.BookWordCountBolt;
import storm.bolts.BookWordRankerBolt;
import storm.bolts.ReportBolt;

class PdfWordCountTopology
{
  public static void main(String[] args) throws Exception
  {
    // create the topology
    TopologyBuilder builder = new TopologyBuilder();

    // attach the spooling spout to the topology - parallelism of 1
    builder.setSpout("spooling-spout", new PdfSpoolingSpout(), 1);

    // remove the unwanted words from each sentence and extract the words - parallelism of 10
    builder.setBolt("word-parse-bolt", new ParseWordBolt(), 10). shuffleGrouping("spooling-spout");

    // count the word appearances by book - parallelism of 10
    builder.setBolt("book-word-count-bolt", new BookWordCountBolt(), 10).fieldsGrouping("word-parse-bolt", new Fields("book-title"));

    // order the top 10 words per book - parallelism of 5
    builder.setBolt("book-word-ranker", new BookWordRankerBolt()).globalGrouping("book-word-count-bolt");

    // report the result to REDIS with a ReportBolt
    builder.setBolt("report-bolt", new ReportBolt(), 1).globalGrouping("book-word-ranker");

    // create the default config object
    Config conf = new Config();

    // set the config in debugging mode
    conf.setDebug(true);

    if (args != null && args.length > 0) {

      // run it in a live cluster

      // set the number of workers for running all spout and bolt tasks
      conf.setNumWorkers(3);

      // create the topology and submit with config
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

    } else {

      // run it in a simulated local cluster

      // set the number of threads to run - similar to setting number of workers in live cluster
      conf.setMaxTaskParallelism(3);

      // create the local cluster instance
      LocalCluster cluster = new LocalCluster();

      // submit the topology to the local cluster
      cluster.submitTopology("pdf-word-count-topology", conf, builder.createTopology());

      // let the topology run for 900 seconds. note topologies never terminate!
      Utils.sleep(900000);

      // now kill the topology
      cluster.killTopology("pdf-word-count-topology");

      // we are done, so shutdown the local cluster
      cluster.shutdown();
    }
  }
}
