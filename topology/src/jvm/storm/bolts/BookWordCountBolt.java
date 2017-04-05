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

import java.util.HashMap;
import java.util.Map;

/**
 * A bolt that counts the words that it receives
 */
public class BookWordCountBolt extends BaseRichBolt
{
  // To output tuples from this bolt to the next stage bolts, if any
  private OutputCollector collector;

  // Map to store the book-word-count relation
  private Map<String, HashMap<String, Long>> bookWords;

  // Map to store word-count relation
  private Map<String, Long> wordCount;

  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {

    // save the collector for emitting tuples
    collector = outputCollector;

    // create and initialize the maps
    bookWords = new HashMap<String, HashMap<String, Long>>();
    wordCount = new HashMap<String, Long>();
  }

  @Override
  public void execute(Tuple tuple)
  {
    // get the book title from the 1st column of incoming tuple
    String bookTitle = tuple.getString(0);

    // get the word from the 2nd column of incoming tuple
    String word = tuple.getString(1);

    // set the default value for count
    Long count = 1L;

    // check if the book is present in the map
    if(bookWords.get(bookTitle) == null){

      // book not present, add it
      bookWords.put(bookTitle, new HashMap<String, Long>());
      // add the first word of the book
      bookWords.get(bookTitle).put(word, count);

    } else {

      // present, check if the word is there
      if(bookWords.get(bookTitle).get(word) == null){

        // book present, but not word
        bookWords.get(bookTitle).put(word, count);

      } else {
        // check word present
        if(!bookWords.get(bookTitle).containsKey(word)){
          bookWords.get(bookTitle).put(word, count);
        } else {
          count = bookWords.get(bookTitle).get(word);
          bookWords.get(bookTitle).put(word, ++count);
        }
      }

    }

    // emit the book, word and count
    collector.emit(new Values(bookTitle, word, count));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
  {
    // tell storm the schema of the output tuple for this spout
    // tuple consists of a three columns called 'book-title', 'word' and 'count'
    outputFieldsDeclarer.declare(new Fields("book-title", "word", "count"));
  }
}
