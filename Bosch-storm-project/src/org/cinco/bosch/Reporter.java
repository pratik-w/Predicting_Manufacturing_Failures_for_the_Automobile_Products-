package org.cinco.bosch;

import java.io.File;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.shade.org.apache.commons.lang.ArrayUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class Reporter extends BaseRichBolt{

	private OutputCollector collector;
	HashMap<Integer,int[]> idScore = new HashMap<Integer,int[]>();
	HashMap<Integer,Integer> idRfid = new HashMap<Integer,Integer>();
	private Date timestamp;
	private PrintWriter logFile;

	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		/*String text = tuple.toString();
		System.out.println(text);
		Fields text2 = tuple.getFields();
		System.out.println("helo"+ text2);
		String tempo = (String) tuple.getValueByField("line");
		System.out.println("helo2"+ tempo);
		String[] temp = tempo.split(" ");
		int id = Integer.parseInt(temp[0]);
		for (int i = 1; i < temp.length;i++)
		{
			System.out.println("print inside loop");
			System.out.println("elements "+ i+ "-" +temp[i]);
			String[] values;
			//values = temp[i].split(":");
			//String feature = "feature_"+temp[0];
			//HashMap<String, Integer> map = new HashMap<String,Integer>();
			//for (int j= 1;j<5;j++)
			//{
			//	String feature = "feature_"+j;
			//	map.put(feature, 0);
			//}
			double[] array;
			array = new double[5];
			Arrays.fill(array, 0.0);*/
		String result;
		System.out.println("INSIDE reporter");
		int id = (int) tuple.getValueByField("id");
		int[] score = (int[]) tuple.getValueByField("score");
		int finalcount = (int) tuple.getValueByField("rfid");
		System.out.println("Printing the values"+id+score+finalcount);
		if (idRfid.containsKey(id))
		{
			System.out.println("He got the key "+ id);
			int temp = idRfid.get(id);
			temp = temp + finalcount;
			int[] tempscore = idScore.get(id);
			//tempscore = (tempscore+score)/2;
			System.out.println("the temp value is "+ temp);
			if (temp == 15)
			{
				int count1=0;
				int count0=0;
				System.out.println("inside the condition");
				int[] finalscore = idScore.get(id);
				for (int k =0 ;k<finalscore.length;k++)
				{
					if (finalscore[k] == 1)
					{
						count1++;
					}
					else
					{
						count0++;
					}
				}
				if (count0>count1)
				{
					result = "PASSED";
				}
				else
				{	result = "FAILED";
				
				}
				System.out.println("AAAAAAAAAAAAA");
				System.out.println("The result of ID : "+id+"  is -- "+result);
				timestamp = new Date();
				logFile.write("\n");
				logFile.write("<"+timestamp+">");
				logFile.write(" Result for Product ID "+id+ " : "+ result);
				logFile.write("\n");
				logFile.flush();
				idRfid.remove(id);
				idScore.remove(id);
				
			}
			else
			{
				System.out.println("he got key but not fullfilled");
				System.out.println(temp);
				System.out.println(tempscore);
				idRfid.put(id, temp);
				int[] newscore = ArrayUtils.addAll(tempscore, score);
				idScore.put(id,newscore);
				
			}
		}
		else
		{
			System.out.println("Inserting into hashmaps");
			idRfid.put(id, finalcount);
			idScore.put(id,score);
			System.out.println(idRfid.toString());
			System.out.println(idScore.toString());
		}
		
	}

	@Override
	public void prepare(Map mao, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		
		idScore = new HashMap<Integer,int[]>();
		idRfid = new HashMap<Integer,Integer>();
		try{
			logFile = new PrintWriter(new File("/s/chopin/a/grad/akmittal/stormlogs/boschop.txt"));
		}catch (Exception e) {
			System.out.println("UNABLE TO WRITE FILE :: 1 ");
			e.printStackTrace();
		}
		
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		// this is the final bolt
	}



	

}
