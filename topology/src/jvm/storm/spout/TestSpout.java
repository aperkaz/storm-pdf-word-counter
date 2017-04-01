package storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

public class TestSpout extends BaseRichSpout {
  SpoutOutputCollector _collector;
  Random _rand;


  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    _rand = new Random();
  }

  @Override
  public void nextTuple() {
    Utils.sleep(1000);
    String[] sentence1 = new String[]{
      "Book1","test the sentence1"
    };

    String[] sentence2 = new String[]{
      "Book2","test the sentence2"
    };

    String[] sentence3 = new String[]{
      "Book3","test the sentence3"
    };

    String[] sentence4 = new String[]{
      "Book4","test the sentence4"
    };

    String[] sentence5 = new String[]{
      "Book5","test the sentence5"
    };

    String[][] bookSentences = new String[][]{
      sentence1,
      sentence2,
      sentence3,
      sentence4,
      sentence5
    };

    int index = _rand.nextInt(bookSentences.length);

    String bookName = bookSentences[index][0];
    String sentenceContent = bookSentences[index][1];

    _collector.emit(new Values(bookName,sentenceContent));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    // tuple consists of a double column ["book-title", "sentence"]
    declarer.declare(new Fields("book-title", "sentence"));
  }

}
