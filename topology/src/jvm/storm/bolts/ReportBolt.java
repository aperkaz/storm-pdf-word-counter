package storm.bolts;

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

import java.util.Map;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;

import storm.tools.*;
import storm.tools.Rankings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * A bolt that prints the word and count to redis
 */
public class ReportBolt extends BaseRichBolt
{
  // place holder to keep the connection to redis
  transient RedisConnection<String,String> redis;

  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {
    // instantiate a redis connection
    RedisClient client = new RedisClient("localhost",6379);

    // initiate the actual connection
    redis = client.connect();
  }

  @Override
  public void execute(Tuple tuple)
  {
    // extract information form tuple ['pdf-title', 'word', 'count']

    String pdfTitle = tuple.getString(0);
    String word = tuple.getString(1);
    Long count = tuple.getLong(2);

    redis.publish("PdfWordCount", pdfTitle + "|" + word + "|" + Long.toString(count));

    System.out.println("\n PUBLISHED TO REDIS: "+ pdfTitle + " | " + word + " | " + count + "\n");
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    // nothing to add - since it is the final bolt
  }
}
