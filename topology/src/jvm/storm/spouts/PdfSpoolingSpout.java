package storm.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.File;

import java.util.Map;
import java.util.Random;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import storm.utils.Book;
import storm.utils.BookLine;
import storm.utils.FileUtils;

/*
*  Spooling spout that reads all the pdfs from the sourceDir
*/
public class PdfSpoolingSpout extends BaseRichSpout {
  SpoutOutputCollector _collector;
  Random _rand;

  ArrayList<File> pdfFileList = new ArrayList<File>();
  HashMap<String, ArrayList<String>> booksContent = new HashMap<String, ArrayList<String>>();

  private static String sourceDir = "/vagrant/books";

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    _rand = new Random();

    // initialize files to spool
    pdfFileList = FileUtils.extractPdfFiles(sourceDir);

		// populate books
    for(File pdfFile : pdfFileList){
		    ArrayList<String>pdfContent = FileUtils.getPdfContent(pdfFile);
        System.out.println("Adding: "+pdfFile.getName());
        booksContent.put(pdfFile.getName(), pdfContent);
    }

  }

  @Override
  public void nextTuple() {
    try{
      Thread.sleep(1);
    } catch (Exception e){
      e.printStackTrace();
    }


    String bookTitle = "", line = "";

    // emit books
    Iterator it = booksContent.entrySet().iterator();
    while (it.hasNext()) {
       Map.Entry pair = (Map.Entry)it.next();

       bookTitle = (String) pair.getKey();
       ArrayList<String> bookContent = (ArrayList<String>) pair.getValue();
       for(String text : bookContent){
         _collector.emit(new Values(bookTitle , text));
       }
       it.remove();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    // tuple consists of a double column ["book-title", "sentence"]
    declarer.declare(new Fields("book-title", "sentence"));
  }

}
