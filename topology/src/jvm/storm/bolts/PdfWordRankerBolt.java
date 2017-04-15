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
import java.util.LinkedList;
import java.util.List;
import java.util.Iterator;

import storm.utils.Word;

/**
 * A bolt that orders the top 10 words per pdf
 */
public class PdfWordRankerBolt extends BaseRichBolt
{
  // To output tuples from this bolt to the next stage bolts, if any
  private OutputCollector collector;

  // number of top N words
  private int TOP_N_WORDS = 10;

  // report count
  private int reportCount = 0;

  // store pdfTitle -> listOfTopNWords
  private HashMap<String, LinkedList<Word>> pdfWordMap;

  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {

    // save the collector for emitting tuples
    collector = outputCollector;

    // create and initialize the data structures
    pdfWordMap = new HashMap<String, LinkedList<Word>>();
  }

  @Override
  public void execute(Tuple tuple)
  {
    // extract values from ['pdf-title', 'word', 'count']
    String pdfTitle = tuple.getString(0);
    String word = tuple.getString(1);
    Long count = tuple.getLong(2);

    // check if the pdf is present in the map
    if(pdfWordMap.get(pdfTitle) == null){

      pdfWordMap.put(pdfTitle, new LinkedList<Word>());

      // add first word
      pdfWordMap.get(pdfTitle).add(new Word(word, count));

    } else {
      // chekc if word cualifies in the topN
      LinkedList<Word> TopNWords = pdfWordMap.get(pdfTitle);
      boolean alreadyInList = false;

      // word allready in the list
      for(int iterator = 0; iterator < TopNWords.size(); iterator++){
        if(TopNWords.get(iterator).getContent().equals(word)){
          pdfWordMap.get(pdfTitle).set(iterator, new Word(word, count));
          alreadyInList = true;
          break;
        }
      }

      // new word to the list
      if(!alreadyInList){
        for(int iterator = 0; iterator < TopNWords.size(); iterator++){
          Word iteratedWord = TopNWords.get(iterator);
          if(iteratedWord.getCount() < count){
            // insert new word word and shift the rest
            pdfWordMap.get(pdfTitle).add(iterator, new Word(word, count));
            break;
          }
        }
      }
    }

    // remove the extra elements in the list if any
    if(pdfWordMap.get(pdfTitle).size() > TOP_N_WORDS)
      pdfWordMap.get(pdfTitle).subList(TOP_N_WORDS, pdfWordMap.get(pdfTitle).size()).clear();

    if(++reportCount >= 100){
      // emit the topN words for all pdfs

      Iterator it = pdfWordMap.entrySet().iterator();
      while (it.hasNext()) {
           Map.Entry pair = (Map.Entry)it.next();
           pdfTitle = (String) pair.getKey();
           LinkedList<Word> wordList = (LinkedList<Word>) pair.getValue();
           for(Word iteratedWord : wordList){
             collector.emit(new Values(pdfTitle, iteratedWord.getContent(), iteratedWord.getCount()));
           }
       }
      reportCount = 0;
    }

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
  {
    // tell storm the schema of the output tuple for this spout
    // tuple consists of a three columns ['pdf-title', 'word', 'count']
    outputFieldsDeclarer.declare(new Fields("pdf-title", "word", "count"));
  }
}
