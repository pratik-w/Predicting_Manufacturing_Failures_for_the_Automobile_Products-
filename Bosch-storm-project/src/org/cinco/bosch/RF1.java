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

public class RF1 extends BaseRichBolt {

	private OutputCollector collector;

	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		double predictor0 = 0;
		double predictor1 = 0;
		int rfid = 1;
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

		//tree 1
		if (feature[767] <= -0.183)
		     if (feature[723] <= -0.241)
		      predictor0= 1.0;
		     else if (feature[723] > -0.241)
		      if (feature[741] <= 0.012)
		       if (feature[767] <= -0.269)
		        if (feature[732] <= 0.154)
		         if (feature[831] <= -0.48)
		          predictor0= 0.0;
		         else if (feature[831] > -0.48)
		          predictor0= 1.0;
		        else if (feature[732] > 0.154)
		         if (feature[842] <= -0.987)
		          predictor0= 0.0;
		         else if (feature[842] > -0.987)
		          predictor0= 1.0;
		       else if (feature[767] > -0.269)
		        if (feature[123] <= 0.0)
		         if (feature[852] <= -0.119)
		          predictor0= 0.0;
		         else if (feature[852] > -0.119)
		          predictor0= 0.0;
		        else if (feature[123] > 0.0)
		         if (feature[758] <= -0.09)
		          predictor0= 1.0;
		         else if (feature[758] > -0.09)
		          predictor0= 0.0;
		      else if (feature[741] > 0.012)
		       if (feature[735] <= -0.143)
		        if (feature[833] <= -0.987)
		         predictor0= 0.0;
		        else if (feature[833] > -0.987)
		         if (feature[763] <= 0.195)
		          predictor0= 1.0;
		         else if (feature[763] > 0.195)
		          predictor0= 0.0;
		       else if (feature[735] > -0.143)
		        if (feature[731] <= -0.126)
		         if (feature[838] <= -0.558)
		          predictor0= 0.0;
		         else if (feature[838] > -0.558)
		          predictor0= 1.0;
		        else if (feature[731] > -0.126)
		         if (feature[3] <= 0.0)
		          predictor0= 0.0;
		         else if (feature[3] > 0.0)
		          predictor0= 0.0;
		    else if (feature[767] > -0.183)
		     if (feature[826] <= -0.003)
		      if (feature[834] <= -0.332)
		       if (feature[736] <= 0.08)
		        if (feature[380] <= 0.155)
		         if (feature[404] <= 0.023)
		          predictor0= 0.0;
		         else if (feature[404] > 0.023)
		          predictor0= 0.0;
		        else if (feature[380] > 0.155)
		         if (feature[747] <= -0.179)
		          predictor0= 0.0;
		         else if (feature[747] > -0.179)
		          predictor0= 1.0;
		       else if (feature[736] > 0.08)
		        if (feature[62] <= -0.015)
		         if (feature[745] <= -0.201)
		          predictor0= 1.0;
		         else if (feature[745] > -0.201)
		          predictor0= 0.0;
		        else if (feature[62] > -0.015)
		         if (feature[392] <= -0.07)
		          predictor0= 0.0;
		         else if (feature[392] > -0.07)
		          predictor0= 0.0;
		      else if (feature[834] > -0.332)
		       if (feature[788] <= -0.015)
		        if (feature[781] <= -0.072)
		         if (feature[751] <= 0.063)
		          predictor0= 1.0;
		         else if (feature[751] > 0.063)
		          predictor0= 0.0;
		        else if (feature[781] > -0.072)
		         if (feature[754] <= -0.106)
		          predictor0= 0.0;
		         else if (feature[754] > -0.106)
		          predictor0= 1.0;
		       else if (feature[788] > -0.015)
		        if (feature[757] <= -0.078)
		         if (feature[48] <= 0.0)
		          predictor0= 1.0;
		         else if (feature[48] > 0.0)
		          predictor0= 0.0;
		        else if (feature[757] > -0.078)
		         if (feature[834] <= -0.011)
		          predictor0= 1.0;
		         else if (feature[834] > -0.011)
		          predictor0= 1.0;
		     else if (feature[826] > -0.003)
		      if (feature[836] <= -0.431)
		       if (feature[725] <= -0.222)
		        if (feature[13] <= -0.129)
		         predictor0= 1.0;
		        else if (feature[13] > -0.129)
		         if (feature[802] <= 0.005)
		          predictor0= 0.0;
		         else if (feature[802] > 0.005)
		          predictor0= 1.0;
		       else if (feature[725] > -0.222)
		        if (feature[386] <= -0.002)
		         if (feature[725] <= 0.408)
		          predictor0= 0.0;
		         else if (feature[725] > 0.408)
		          predictor0= 1.0;
		        else if (feature[386] > -0.002)
		         if (feature[303] <= -0.013)
		          predictor0= 0.0;
		         else if (feature[303] > -0.013)
		          predictor0= 0.0;
		      else if (feature[836] > -0.431)
		       if (feature[763] <= 0.05)
		        if (feature[799] <= -0.101)
		         if (feature[740] <= -0.072)
		          predictor0= 1.0;
		         else if (feature[740] > -0.072)
		          predictor0= 1.0;
		        else if (feature[799] > -0.101)
		         if (feature[771] <= 0.0)
		          predictor0= 1.0;
		         else if (feature[771] > 0.0)
		          predictor0= 1.0;
		       else if (feature[763] > 0.05)
		        if (feature[819] <= 0.003)
		         if (feature[87] <= 0.002)
		          predictor0= 1.0;
		         else if (feature[87] > 0.002)
		          predictor0= 0.0;
		        else if (feature[819] > 0.003)
		         if (feature[843] <= -0.001)
		          predictor0= 1.0;
		         else if (feature[843] > -0.001)
		          predictor0= 1.0;
		
		// tree2
		if (feature[4] <= 0.0)
		     if (feature[818] <= -0.003)
		      if (feature[12] <= -0.002)
		       if (feature[826] <= 0.043)
		        if (feature[57] <= 0.001)
		         if (feature[837] <= 0.007)
		          predictor1= 0.0;
		         else if (feature[837] > 0.007)
		          predictor1= 0.0;
		        else if (feature[57] > 0.001)
		         if (feature[843] <= -0.416)
		          predictor1= 0.0;
		         else if (feature[843] > -0.416)
		          predictor1= 1.0;
		       else if (feature[826] > 0.043)
		        if (feature[836] <= -0.431)
		         if (feature[9] <= -0.121)
		          predictor1= 0.0;
		         else if (feature[9] > -0.121)
		          predictor1= 0.0;
		        else if (feature[836] > -0.431)
		         if (feature[3] <= 0.003)
		          predictor1= 1.0;
		         else if (feature[3] > 0.003)
		          predictor1= 1.0;
		      else if (feature[12] > -0.002)
		       if (feature[836] <= -0.431)
		        if (feature[325] <= 0.0)
		         if (feature[13] <= 0.167)
		          predictor1= 0.0;
		         else if (feature[13] > 0.167)
		          predictor1= 0.0;
		        else if (feature[325] > 0.0)
		         if (feature[771] <= 0.047)
		          predictor1= 0.0;
		         else if (feature[771] > 0.047)
		          predictor1= 1.0;
		       else if (feature[836] > -0.431)
		        if (feature[815] <= 0.032)
		         if (feature[33] <= 0.005)
		          predictor1= 1.0;
		         else if (feature[33] > 0.005)
		          predictor1= 1.0;
		        else if (feature[815] > 0.032)
		         if (feature[727] <= -0.119)
		          predictor1= 0.0;
		         else if (feature[727] > -0.119)
		          predictor1= 1.0;
		     else if (feature[818] > -0.003)
		      if (feature[767] <= -0.097)
		       if (feature[852] <= 0.0)
		        if (feature[737] <= 0.15)
		         if (feature[840] <= -0.987)
		          predictor1= 0.0;
		         else if (feature[840] > -0.987)
		          predictor1= 1.0;
		        else if (feature[737] > 0.15)
		         if (feature[18] <= 0.009)
		          predictor1= 0.0;
		         else if (feature[18] > 0.009)
		          predictor1= 1.0;
		       else if (feature[852] > 0.0)
		        if (feature[737] <= 0.15)
		         if (feature[779] <= -0.003)
		          predictor1= 0.0;
		         else if (feature[779] > -0.003)
		          predictor1= 0.0;
		        else if (feature[737] > 0.15)
		         if (feature[761] <= -0.081)
		          predictor1= 0.0;
		         else if (feature[761] > -0.081)
		          predictor1= 0.0;
		      else if (feature[767] > -0.097)
		       if (feature[945] <= -0.002)
		        predictor1= 1.0;
		       else if (feature[945] > -0.002)
		        if (feature[834] <= -0.332)
		         if (feature[365] <= 0.003)
		          predictor1= 0.0;
		         else if (feature[365] > 0.003)
		          predictor1= 0.0;
		        else if (feature[834] > -0.332)
		         if (feature[752] <= 0.051)
		          predictor1= 1.0;
		         else if (feature[752] > 0.051)
		          predictor1= 1.0;
		    else if (feature[4] > 0.0)
		     if (feature[798] <= -0.002)
		      if (feature[789] <= -0.021)
		       if (feature[733] <= 0.062)
		        if (feature[833] <= -0.987)
		         if (feature[729] <= 0.013)
		          predictor1= 0.0;
		         else if (feature[729] > 0.013)
		          predictor1= 0.0;
		        else if (feature[833] > -0.987)
		         if (feature[38] <= 0.096)
		          predictor1= 1.0;
		         else if (feature[38] > 0.096)
		          predictor1= 1.0;
		       else if (feature[733] > 0.062)
		        if (feature[689] <= -0.035)
		         predictor1= 1.0;
		        else if (feature[689] > -0.035)
		         if (feature[831] <= -0.052)
		          predictor1= 0.0;
		         else if (feature[831] > -0.052)
		          predictor1= 1.0;
		      else if (feature[789] > -0.021)
		       if (feature[11] <= -0.2)
		        if (feature[818] <= -0.128)
		         if (feature[5] <= 0.07)
		          predictor1= 1.0;
		         else if (feature[5] > 0.07)
		          predictor1= 1.0;
		        else if (feature[818] > -0.128)
		         if (feature[730] <= -0.027)
		          predictor1= 1.0;
		         else if (feature[730] > -0.027)
		          predictor1= 0.0;
		       else if (feature[11] > -0.2)
		        if (feature[31] <= 0.011)
		         if (feature[842] <= -0.987)
		          predictor1= 0.0;
		         else if (feature[842] > -0.987)
		          predictor1= 1.0;
		        else if (feature[31] > 0.011)
		         if (feature[746] <= 0.0)
		          predictor1= 1.0;
		         else if (feature[746] > 0.0)
		          predictor1= 1.0;
		     else if (feature[798] > -0.002)
		      if (feature[11] <= 0.182)
		       if (feature[752] <= -0.657)
		        if (feature[848] <= -0.003)
		         if (feature[725] <= 0.444)
		          predictor1= 0.0;
		         else if (feature[725] > 0.444)
		          predictor1= 1.0;
		        else if (feature[848] > -0.003)
		         if (feature[51] <= 0.0)
		          predictor1= 0.0;
		         else if (feature[51] > 0.0)
		          predictor1= 0.0;
		       else if (feature[752] > -0.657)
		        if (feature[832] <= -0.763)
		         if (feature[51] <= 0.039)
		          predictor1= 0.0;
		         else if (feature[51] > 0.039)
		          predictor1= 0.0;
		        else if (feature[832] > -0.763)
		         if (feature[7] <= -0.152)
		          predictor1= 1.0;
		         else if (feature[7] > -0.152)
		          predictor1= 1.0;
		      else if (feature[11] > 0.182)
		       if (feature[749] <= -0.032)
		        if (feature[806] <= 0.005)
		         if (feature[841] <= 0.003)
		          predictor1= 0.0;
		         else if (feature[841] > 0.003)
		          predictor1= 0.0;
		        else if (feature[806] > 0.005)
		         if (feature[2] <= -0.197)
		          predictor1= 0.0;
		         else if (feature[2] > -0.197)
		          predictor1= 1.0;
		       else if (feature[749] > -0.032)
		        if (feature[836] <= -0.065)
		         if (feature[749] <= 0.182)
		          predictor1= 0.0;
		         else if (feature[749] > 0.182)
		          predictor1= 0.0;
		        else if (feature[836] > -0.065)
		         if (feature[763] <= 0.118)
		          predictor1= 1.0;
		         else if (feature[763] > 0.118)
		          predictor1= 0.0;
		
		int[] finalpredictor;
		finalpredictor = new int[2];

		finalpredictor[0] = (int) predictor0;
		finalpredictor[1] = (int) predictor1;
		

		this.collector.emit(new Values(tuple.getValueByField("id"), finalpredictor, rfid));

	}

	
	

	@Override
	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("id", "score", "rfid"));
	}

}
