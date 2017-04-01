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

import java.util.Map;
import java.util.Arrays;

/**
 * A bolt that parses a given line into words
 */
public class ParseWordBolt extends BaseRichBolt
{
  // To output tuples from this bolt to the count bolt
  OutputCollector collector;

  private String[] skipWords = {"rt", "to", "me","la","on","that","que",
    "followers","watch","know","not","have","like","I'm","new","good","do",
    "more","es","te","followers","Followers","las","you","and","de","my","is",
    "en","una","in","for","this","go","en","all","no","don't","up","are",
    "http","http:","https","https:","http://","https://","with","just","your",
    "para","want","your","you're","really","video","it's","when","they","their","much",
    "would","what","them","todo","FOLLOW","retweet","RETWEET","even","right","like",
    "bien","Like","will","Will","pero","Pero","can't","were","Can't","Were",
    "make","take","This","from","about","como","esta","follows","followed"};

  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {
    // save the output collector for emitting tuples
    collector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple)
  {
    // get the 1st column 'book' from tuple
    String book = tuple.getString(0);

    // get the 2st column 'sentenceContent' from tuple
    String sentenceContent = tuple.getString(1);

    // provide the delimiters for splitting the given sentence
    String delims = "[ .,?!]+";

    // now split the sentence into tokens
    String[] tokens = sentenceContent.split(delims);

    // for each token/word, emit it
    for (String token: tokens) {
      //emit only words greater than length 3 and not stopword list
      if(token.length() > 3 && !Arrays.asList(skipWords).contains(token)){
        // emit the word if bigger than 3 and not in the skip list
        collector.emit(new Values(book, token));
      }
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    // tuple consists of a double column ["book-title", "word"]
    declarer.declare(new Fields("book-title", "word"));
  }

}
