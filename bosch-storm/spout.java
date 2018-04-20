package org.cinco.bosch;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class spout extends BaseRichSpout{
	
	private String filepath;
	private String fileName;
	private static final Logger LOG = LoggerFactory.getLogger(spout.class);
	private SpoutOutputCollector _collector;
	private BufferedReader reader;
	private AtomicLong linesRead;
	
	public spout(String filepath)
	{
		if (filepath == null)
		{
			throw new RuntimeException("ERROR: THE FILE PATH IS MISSING. Please enter a valid file path");
		}
		this.filepath = filepath;
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		try {
		      String line = reader.readLine();
		      if (line != null) {
		        long id = linesRead.incrementAndGet();
		        _collector.emit(new Values(line), id);
		      } else {
		        System.out.println("Finished reading file, " + linesRead.get() + " lines read");
		        Thread.sleep(10000);
		      }
		    } catch (Exception e) {
		      e.printStackTrace();
		}
	}

	@Override
	public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
		linesRead = new AtomicLong(0);
	    _collector = collector;
	    try {
	      //fileName= (String) map.get(filepath);
	      this.fileName = "/s/chopin/a/grad/akmittal/Downloads/stormsample.libsvm";
	      System.out.println("XXXXXXX");
	      System.out.println(fileName);
	      reader = new BufferedReader(new FileReader(fileName));
	      // read and ignore the header if one exists
	    } catch (Exception e) {
	      throw new RuntimeException(e);
	}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputDeclarer) {
		// TODO Auto-generated method stub
		outputDeclarer.declare(new Fields("line"));
		
	}

	
	
}
