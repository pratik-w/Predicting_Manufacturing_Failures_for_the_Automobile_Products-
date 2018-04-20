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

public class RF3 extends BaseRichBolt{

	private OutputCollector collector;
	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		double predictor4 = 0;
		double predictor5 = 0;
		int rfid = 3;
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
		
		//Tree5
		if (feature[810] <= -0.001)
		     if (feature[791] <= 0.07)
		      if (feature[764] <= 0.0)
		       if (feature[848] <= 0.006)
		        if (feature[839] <= -0.709)
		         if (feature[306] <= -0.001)
		          predictor4= 1.0;
		         else if (feature[306] > -0.001)
		          predictor4= 0.0;
		        else if (feature[839] > -0.709)
		         if (feature[726] <= -0.002)
		          predictor4= 1.0;
		         else if (feature[726] > -0.002)
		          predictor4= 1.0;
		       else if (feature[848] > 0.006)
		        if (feature[792] <= 0.145)
		         if (feature[378] <= 0.0)
		          predictor4= 1.0;
		         else if (feature[378] > 0.0)
		          predictor4= 1.0;
		        else if (feature[792] > 0.145)
		         if (feature[784] <= 0.119)
		          predictor4= 0.0;
		         else if (feature[784] > 0.119)
		          predictor4= 1.0;
		      else if (feature[764] > 0.0)
		       if (feature[833] <= -0.987)
		        if (feature[32] <= 0.018)
		         if (feature[82] <= 0.141)
		          predictor4= 0.0;
		         else if (feature[82] > 0.141)
		          predictor4= 1.0;
		        else if (feature[32] > 0.018)
		         if (feature[757] <= 0.021)
		          predictor4= 0.0;
		         else if (feature[757] > 0.021)
		          predictor4= 1.0;
		       else if (feature[833] > -0.987)
		        predictor4= 1.0;
		     else if (feature[791] > 0.07)
		      if (feature[830] <= -0.969)
		       if (feature[798] <= -0.035)
		        if (feature[6] <= -0.03)
		         predictor4= 1.0;
		        else if (feature[6] > -0.03)
		         predictor4= 0.0;
		       else if (feature[798] > -0.035)
		        predictor4= 0.0;
		      else if (feature[830] > -0.969)
		       if (feature[371] <= 0.116)
		        if (feature[770] <= 0.038)
		         if (feature[388] <= -0.018)
		          predictor4= 1.0;
		         else if (feature[388] > -0.018)
		          predictor4= 1.0;
		        else if (feature[770] > 0.038)
		         predictor4= 0.0;
		       else if (feature[371] > 0.116)
		        predictor4= 0.0;
		    else if (feature[810] > -0.001)
		     if (feature[751] <= -0.648)
		      if (feature[725] <= -0.222)
		       if (feature[831] <= -0.035)
		        if (feature[5] <= -0.112)
		         if (feature[73] <= 0.0)
		          predictor4= 0.0;
		         else if (feature[73] > 0.0)
		          predictor4= 1.0;
		        else if (feature[5] > -0.112)
		         if (feature[732] <= -0.185)
		          predictor4= 0.0;
		         else if (feature[732] > -0.185)
		          predictor4= 0.0;
		       else if (feature[831] > -0.035)
		        if (feature[757] <= 0.03)
		         if (feature[85] <= 0.0)
		          predictor4= 0.0;
		         else if (feature[85] > 0.0)
		          predictor4= 0.0;
		        else if (feature[757] > 0.03)
		         if (feature[91] <= -0.001)
		          predictor4= 1.0;
		         else if (feature[91] > -0.001)
		          predictor4= 0.0;
		      else if (feature[725] > -0.222)
		       if (feature[688] <= 0.034)
		        if (feature[249] <= -0.008)
		         predictor4= 1.0;
		        else if (feature[249] > -0.008)
		         if (feature[732] <= -0.185)
		          predictor4= 0.0;
		         else if (feature[732] > -0.185)
		          predictor4= 0.0;
		       else if (feature[688] > 0.034)
		        if (feature[781] <= 0.059)
		         predictor4= 0.0;
		        else if (feature[781] > 0.059)
		         predictor4= 1.0;
		     else if (feature[751] > -0.648)
		      if (feature[849] <= -0.029)
		       if (feature[776] <= -0.004)
		        if (feature[326] <= 0.0)
		         if (feature[810] <= 0.002)
		          predictor4= 0.0;
		         else if (feature[810] > 0.002)
		          predictor4= 0.0;
		        else if (feature[326] > 0.0)
		         if (feature[791] <= -0.092)
		          predictor4= 0.0;
		         else if (feature[791] > -0.092)
		          predictor4= 1.0;
		       else if (feature[776] > -0.004)
		        if (feature[792] <= -0.001)
		         if (feature[804] <= -0.009)
		          predictor4= 0.0;
		         else if (feature[804] > -0.009)
		          predictor4= 1.0;
		        else if (feature[792] > -0.001)
		         if (feature[770] <= -0.009)
		          predictor4= 1.0;
		         else if (feature[770] > -0.009)
		          predictor4= 1.0;
		      else if (feature[849] > -0.029)
		       if (feature[853] <= 0.0)
		        if (feature[833] <= -0.987)
		         if (feature[818] <= 0.077)
		          predictor4= 0.0;
		         else if (feature[818] > 0.077)
		          predictor4= 1.0;
		        else if (feature[833] > -0.987)
		         predictor4= 1.0;
		       else if (feature[853] > 0.0)
		        if (feature[843] <= -0.416)
		         if (feature[827] <= 0.144)
		          predictor4= 0.0;
		         else if (feature[827] > 0.144)
		          predictor4= 0.0;
		        else if (feature[843] > -0.416)
		         if (feature[827] <= -0.028)
		          predictor4= 1.0;
		         else if (feature[827] > -0.028)
		          predictor4= 1.0;
		
		//tree 6
		 if (feature[795] <= -0.005)
		     if (feature[392] <= 0.002)
		      if (feature[704] <= 0.004)
		       if (feature[841] <= -0.306)
		        if (feature[291] <= -0.003)
		         predictor5= 1.0;
		        else if (feature[291] > -0.003)
		         if (feature[754] <= -0.096)
		          predictor5= 0.0;
		         else if (feature[754] > -0.096)
		          predictor5= 0.0;
		       else if (feature[841] > -0.306)
		        if (feature[815] <= -0.014)
		         if (feature[769] <= -0.009)
		          predictor5= 1.0;
		         else if (feature[769] > -0.009)
		          predictor5= 1.0;
		        else if (feature[815] > -0.014)
		         if (feature[838] <= 0.069)
		          predictor5= 1.0;
		         else if (feature[838] > 0.069)
		          predictor5= 1.0;
		      else if (feature[704] > 0.004)
		       if (feature[698] <= 0.116)
		        if (feature[463] <= -0.003)
		         if (feature[742] <= -0.034)
		          predictor5= 1.0;
		         else if (feature[742] > -0.034)
		          predictor5= 0.0;
		        else if (feature[463] > -0.003)
		         if (feature[739] <= 0.023)
		          predictor5= 1.0;
		         else if (feature[739] > 0.023)
		          predictor5= 1.0;
		       else if (feature[698] > 0.116)
		        if (feature[842] <= -0.987)
		         predictor5= 0.0;
		        else if (feature[842] > -0.987)
		         predictor5= 1.0;
		     else if (feature[392] > 0.002)
		      if (feature[840] <= -0.987)
		       if (feature[818] <= -0.191)
		        if (feature[754] <= 0.115)
		         predictor5= 0.0;
		        else if (feature[754] > 0.115)
		         predictor5= 1.0;
		       else if (feature[818] > -0.191)
		        if (feature[740] <= 0.083)
		         predictor5= 0.0;
		        else if (feature[740] > 0.083)
		         if (feature[724] <= -0.051)
		          predictor5= 0.0;
		         else if (feature[724] > -0.051)
		          predictor5= 0.0;
		      else if (feature[840] > -0.987)
		       if (feature[766] <= -0.48)
		        predictor5= 1.0;
		       else if (feature[766] > -0.48)
		        if (feature[788] <= -0.093)
		         if (feature[688] <= 0.016)
		          predictor5= 1.0;
		         else if (feature[688] > 0.016)
		          predictor5= 0.0;
		        else if (feature[788] > -0.093)
		         if (feature[340] <= 0.01)
		          predictor5= 1.0;
		         else if (feature[340] > 0.01)
		          predictor5= 0.0;
		    else if (feature[795] > -0.005)
		     if (feature[751] <= -0.648)
		      if (feature[210] <= -0.006)
		       predictor5= 1.0;
		      else if (feature[210] > -0.006)
		       if (feature[848] <= -0.003)
		        if (feature[865] <= 0.03)
		         if (feature[725] <= -0.222)
		          predictor5= 0.0;
		         else if (feature[725] > -0.222)
		          predictor5= 0.0;
		        else if (feature[865] > 0.03)
		         predictor5= 1.0;
		       else if (feature[848] > -0.003)
		        if (feature[159] <= 0.007)
		         if (feature[739] <= 0.038)
		          predictor5= 0.0;
		         else if (feature[739] > 0.038)
		          predictor5= 0.0;
		        else if (feature[159] > 0.007)
		         if (feature[874] <= 0.088)
		          predictor5= 0.0;
		         else if (feature[874] > 0.088)
		          predictor5= 0.0;
		     else if (feature[751] > -0.648)
		      if (feature[937] <= 0.0)
		       if (feature[842] <= -0.987)
		        if (feature[202] <= -0.006)
		         predictor5= 1.0;
		        else if (feature[202] > -0.006)
		         if (feature[384] <= 0.036)
		          predictor5= 0.0;
		         else if (feature[384] > 0.036)
		          predictor5= 0.0;
		       else if (feature[842] > -0.987)
		        if (feature[802] <= -0.002)
		         if (feature[792] <= 0.064)
		          predictor5= 1.0;
		         else if (feature[792] > 0.064)
		          predictor5= 1.0;
		        else if (feature[802] > -0.002)
		         if (feature[730] <= -0.053)
		          predictor5= 1.0;
		         else if (feature[730] > -0.053)
		          predictor5= 1.0;
		      else if (feature[937] > 0.0)
		       predictor5= 1.0;
		
		int[] finalpredictor;
		finalpredictor = new int[2];
		

		finalpredictor[0] = (int) predictor4;
		finalpredictor[1] = (int) predictor5;
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