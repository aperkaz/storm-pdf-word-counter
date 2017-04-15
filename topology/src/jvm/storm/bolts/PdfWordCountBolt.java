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
public class PdfWordCountBolt extends BaseRichBolt
{
  // To output tuples from this bolt to the next stage bolts, if any
  private OutputCollector collector;

  // Map to store the pdf-word-count relation
  private Map<String, HashMap<String, Long>> pdfWords;

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
    pdfWords = new HashMap<String, HashMap<String, Long>>();
    wordCount = new HashMap<String, Long>();
  }

  @Override
  public void execute(Tuple tuple)
  {
    // get the pdf title from the 1st column of incoming tuple
    String pdfTitle = tuple.getString(0);

    // get the word from the 2nd column of incoming tuple
    String word = tuple.getString(1);

    // set the default value for count
    Long count = 1L;

    // check if the pdf is present in the map
    if(pdfWords.get(pdfTitle) == null){

      // pdf not present, add it
      pdfWords.put(pdfTitle, new HashMap<String, Long>());
      // add the first word of the pdf
      pdfWords.get(pdfTitle).put(word, count);

    } else {

      // present, check if the word is there
      if(pdfWords.get(pdfTitle).get(word) == null){

        // pdf present, but not word
        pdfWords.get(pdfTitle).put(word, count);

      } else {
        // check word present
        if(!pdfWords.get(pdfTitle).containsKey(word)){
          pdfWords.get(pdfTitle).put(word, count);
        } else {
          count = pdfWords.get(pdfTitle).get(word);
          pdfWords.get(pdfTitle).put(word, ++count);
        }
      }

    }

    // emit the pdf, word and count
    collector.emit(new Values(pdfTitle, word, count));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
  {
    // tell storm the schema of the output tuple for this spout
    // tuple consists of a three columns ['pdf-title', 'word', 'count']
    outputFieldsDeclarer.declare(new Fields("pdf-title", "word", "count"));
  }
}
