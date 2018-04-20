package org.cinco.bosch;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class IdExtractor extends BaseRichBolt{

	private OutputCollector collector;
	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		String tempo = (String) tuple.getValueByField("line");
		String[] values = tempo.split(" ");
		System.out.println("KKKKK"+ values[0]);
		int id = Integer.parseInt(values[0]);
		String[] data;
		data = new String[values.length-1];
		int j =0;
		for (int i =1;i<values.length;i++)
		{
			data[j] = values[i];
			j++;
		}
		this.collector.emit(new Values(id,data));
	}

	@Override
	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("id","data"));
		
	}
	

}
