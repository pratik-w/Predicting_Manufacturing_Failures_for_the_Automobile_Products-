package org.cinco.bosch;

import java.util.Arrays;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class RF5 extends BaseRichBolt{

	private OutputCollector collector;
	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
			double predictor8 = 0;
			
			int rfid = 5;
			// double finalpredictor;
			String[] tesco = (String[]) tuple.getValueByField("data");
			double[] feature;
			feature = new double[969];
			Arrays.fill(feature, 0.0);
			for (int i = 0; i < tesco.length; i++) {
				String[] splits = tesco[i].split(":");
				int index = Integer.parseInt(splits[0]);
				feature[index] = Double.parseDouble(splits[1]);
			}
			
		
		// Tree9
			
			if (feature[798] <= 0.002)
			     if (feature[803] <= 0.0)
			      if (feature[793] <= -0.018)
			       if (feature[781] <= -0.072)
			        if (feature[841] <= -0.306)
			         if (feature[828] <= 0.088)
			          predictor8= 0.0;
			         else if (feature[828] > 0.088)
			          predictor8= 0.0;
			        else if (feature[841] > -0.306)
			         if (feature[771] <= 0.012)
			          predictor8= 1.0;
			         else if (feature[771] > 0.012)
			          predictor8= 0.0;
			       else if (feature[781] > -0.072)
			        if (feature[834] <= -0.332)
			         if (feature[786] <= 0.029)
			          predictor8= 0.0;
			         else if (feature[786] > 0.029)
			          predictor8= 0.0;
			        else if (feature[834] > -0.332)
			         if (feature[766] <= -0.48)
			          predictor8= 1.0;
			         else if (feature[766] > -0.48)
			          predictor8= 1.0;
			      else if (feature[793] > -0.018)
			       if (feature[768] <= 0.0)
			        if (feature[764] <= 0.066)
			         if (feature[840] <= -0.987)
			          predictor8= 0.0;
			         else if (feature[840] > -0.987)
			          predictor8= 1.0;
			        else if (feature[764] > 0.066)
			         if (feature[90] <= 0.026)
			          predictor8= 1.0;
			         else if (feature[90] > 0.026)
			          predictor8= 0.0;
			       else if (feature[768] > 0.0)
			        if (feature[752] <= -0.648)
			         if (feature[42] <= -0.657)
			          predictor8= 1.0;
			         else if (feature[42] > -0.657)
			          predictor8= 0.0;
			        else if (feature[752] > -0.648)
			         if (feature[830] <= -0.969)
			          predictor8= 0.0;
			         else if (feature[830] > -0.969)
			          predictor8= 1.0;
			     else if (feature[803] > 0.0)
			      if (feature[766] <= -0.48)
			       if (feature[830] <= -0.969)
			        if (feature[762] <= 0.294)
			         if (feature[48] <= 0.019)
			          predictor8= 0.0;
			         else if (feature[48] > 0.019)
			          predictor8= 0.0;
			        else if (feature[762] > 0.294)
			         if (feature[734] <= 0.0)
			          predictor8= 0.0;
			         else if (feature[734] > 0.0)
			          predictor8= 1.0;
			       else if (feature[830] > -0.969)
			        predictor8= 1.0;
			      else if (feature[766] > -0.48)
			       if (feature[838] <= -0.558)
			        if (feature[703] <= 0.0)
			         if (feature[378] <= -0.07)
			          predictor8= 1.0;
			         else if (feature[378] > -0.07)
			          predictor8= 0.0;
			        else if (feature[703] > 0.0)
			         predictor8= 1.0;
			       else if (feature[838] > -0.558)
			        if (feature[741] <= 0.012)
			         if (feature[743] <= -0.26)
			          predictor8= 1.0;
			         else if (feature[743] > -0.26)
			          predictor8= 1.0;
			        else if (feature[741] > 0.012)
			         if (feature[44] <= 0.0)
			          predictor8= 1.0;
			         else if (feature[44] > 0.0)
			          predictor8= 0.0;
			    else if (feature[798] > 0.002)
			     if (feature[735] <= 0.0)
			      if (feature[836] <= -0.431)
			       if (feature[753] <= 0.166)
			        if (feature[380] <= 0.006)
			         if (feature[0] <= 0.101)
			          predictor8= 0.0;
			         else if (feature[0] > 0.101)
			          predictor8= 0.0;
			        else if (feature[380] > 0.006)
			         if (feature[795] <= -0.005)
			          predictor8= 0.0;
			         else if (feature[795] > -0.005)
			          predictor8= 0.0;
			       else if (feature[753] > 0.166)
			        if (feature[757] <= 0.084)
			         predictor8= 0.0;
			        else if (feature[757] > 0.084)
			         predictor8= 1.0;
			      else if (feature[836] > -0.431)
			       if (feature[752] <= 0.063)
			        if (feature[743] <= -0.127)
			         if (feature[87] <= -0.002)
			          predictor8= 0.0;
			         else if (feature[87] > -0.002)
			          predictor8= 1.0;
			        else if (feature[743] > -0.127)
			         if (feature[99] <= -0.004)
			          predictor8= 0.0;
			         else if (feature[99] > -0.004)
			          predictor8= 1.0;
			       else if (feature[752] > 0.063)
			        if (feature[38] <= 0.096)
			         if (feature[741] <= 0.012)
			          predictor8= 1.0;
			         else if (feature[741] > 0.012)
			          predictor8= 0.0;
			        else if (feature[38] > 0.096)
			         if (feature[57] <= 0.007)
			          predictor8= 1.0;
			         else if (feature[57] > 0.007)
			          predictor8= 0.0;
			     else if (feature[735] > 0.0)
			      if (feature[838] <= -0.558)
			       if (feature[168] <= 0.025)
			        if (feature[755] <= -0.162)
			         if (feature[341] <= -0.008)
			          predictor8= 1.0;
			         else if (feature[341] > -0.008)
			          predictor8= 0.0;
			        else if (feature[755] > -0.162)
			         if (feature[623] <= 0.032)
			          predictor8= 0.0;
			         else if (feature[623] > 0.032)
			          predictor8= 1.0;
			       else if (feature[168] > 0.025)
			        predictor8= 1.0;
			      else if (feature[838] > -0.558)
			       predictor8= 1.0;

			int[] finalpredictor;
		finalpredictor = new int[1];
		

		finalpredictor[0] = (int) predictor8;
		
		
		this.collector.emit(new Values (tuple.getValueByField("id"), finalpredictor, rfid));
		
		
	}

	@Override
	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("id","score","rfid"));
	}

}