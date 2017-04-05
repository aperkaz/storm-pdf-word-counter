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

import storm.Word;

/**
 * A bolt that orders the top 3 words per book
 */
public class BookWordRankerBolt extends BaseRichBolt
{
  // To output tuples from this bolt to the next stage bolts, if any
  private OutputCollector collector;

  // number of top N words
  private int TOP_N_WORDS = 1;

  // report count
  private int reportCount = 0;

  // store bookTitle -> listOfTopNWords
  private HashMap<String, LinkedList<Word>> bookWordMap;

  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {

    // save the collector for emitting tuples
    collector = outputCollector;

    // create and initialize the data structures
    bookWordMap = new HashMap<String, LinkedList<Word>>();
  }

  @Override
  public void execute(Tuple tuple)
  {
    // extract values from ['book-title', 'word', 'count']
    String bookTitle = tuple.getString(0);
    String word = tuple.getString(1);
    Long count = tuple.getLong(2);

    // check if the book is present in the map
    if(bookWordMap.get(bookTitle) == null){

      bookWordMap.put(bookTitle, new LinkedList<Word>());

      // add first word
      bookWordMap.get(bookTitle).add(new Word(word, count));

    } else {
      // chekc if word cualifies in the topN
      LinkedList<Word> TopNWords = bookWordMap.get(bookTitle);
      boolean alreadyInList = false;

      // word allready in the list
      for(int iterator = 0; iterator < TopNWords.size(); iterator++){
        if(TopNWords.get(iterator).getContent().equals(word)){
          bookWordMap.get(bookTitle).set(iterator, new Word(word, count));
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
            bookWordMap.get(bookTitle).add(iterator, new Word(word, count));
            break;
          }
        }
      }
    }

    // remove the extra elements in the list if any
    if(bookWordMap.get(bookTitle).size() > TOP_N_WORDS)
      bookWordMap.get(bookTitle).subList(TOP_N_WORDS, bookWordMap.get(bookTitle).size()).clear();

    if(++reportCount >= 3){
      // emit the topN words for all books

      Iterator it = bookWordMap.entrySet().iterator();
      while (it.hasNext()) {
           Map.Entry pair = (Map.Entry)it.next();
           bookTitle = (String) pair.getKey();
           LinkedList<Word> wordList = (LinkedList<Word>) pair.getValue();
           for(Word iteratedWord : wordList){
             collector.emit(new Values(bookTitle, iteratedWord.getContent(), iteratedWord.getCount()));
           }
       }
      reportCount = 0;
    }

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
  {
    // tell storm the schema of the output tuple for this spout
    // tuple consists of a three columns called 'book-title', 'word' and 'count'
    outputFieldsDeclarer.declare(new Fields("book-title", "word", "count"));
  }
}
