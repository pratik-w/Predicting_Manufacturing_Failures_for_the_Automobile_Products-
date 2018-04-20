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

public class RF4 extends BaseRichBolt{

	private OutputCollector collector;
	@Override
	public void execute(Tuple tuple) {
		double predictor6 = 0;
		double predictor7 = 0;
		int rfid = 4;
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
		// tree 7
		 if (feature[795] <= -0.005)
		     if (feature[769] <= -0.014)
		      if (feature[79] <= -0.155)
		       if (feature[778] <= -0.005)
		        if (feature[751] <= 0.057)
		         predictor6= 1.0;
		        else if (feature[751] > 0.057)
		         if (feature[0] <= -0.114)
		          predictor6= 1.0;
		         else if (feature[0] > -0.114)
		          predictor6= 0.0;
		       else if (feature[778] > -0.005)
		        predictor6= 1.0;
		      else if (feature[79] > -0.155)
		       if (feature[2] <= 0.276)
		        if (feature[826] <= 0.127)
		         if (feature[102] <= 0.069)
		          predictor6= 1.0;
		         else if (feature[102] > 0.069)
		          predictor6= 0.0;
		        else if (feature[826] > 0.127)
		         if (feature[835] <= -0.987)
		          predictor6= 0.0;
		         else if (feature[835] > -0.987)
		          predictor6= 1.0;
		       else if (feature[2] > 0.276)
		        if (feature[739] <= 0.023)
		         if (feature[831] <= -0.48)
		          predictor6= 0.0;
		         else if (feature[831] > -0.48)
		          predictor6= 1.0;
		        else if (feature[739] > 0.023)
		         if (feature[785] <= -0.056)
		          predictor6= 1.0;
		         else if (feature[785] > -0.056)
		          predictor6= 0.0;
		     else if (feature[769] > -0.014)
		      if (feature[765] <= 0.0)
		       if (feature[839] <= -0.709)
		        if (feature[383] <= 0.027)
		         if (feature[317] <= -0.002)
		          predictor6= 1.0;
		         else if (feature[317] > -0.002)
		          predictor6= 0.0;
		        else if (feature[383] > 0.027)
		         if (feature[351] <= 0.0)
		          predictor6= 1.0;
		         else if (feature[351] > 0.0)
		          predictor6= 0.0;
		       else if (feature[839] > -0.709)
		        if (feature[688] <= 0.025)
		         if (feature[1] <= 0.041)
		          predictor6= 1.0;
		         else if (feature[1] > 0.041)
		          predictor6= 1.0;
		        else if (feature[688] > 0.025)
		         if (feature[829] <= 0.189)
		          predictor6= 1.0;
		         else if (feature[829] > 0.189)
		          predictor6= 0.0;
		      else if (feature[765] > 0.0)
		       if (feature[775] <= 0.001)
		        if (feature[842] <= -0.987)
		         if (feature[764] <= 0.054)
		          predictor6= 0.0;
		         else if (feature[764] > 0.054)
		          predictor6= 0.0;
		        else if (feature[842] > -0.987)
		         if (feature[753] <= -0.19)
		          predictor6= 0.0;
		         else if (feature[753] > -0.19)
		          predictor6= 1.0;
		       else if (feature[775] > 0.001)
		        predictor6= 0.0;
		    else if (feature[795] > -0.005)
		     if (feature[811] <= 0.0)
		      if (feature[791] <= 0.0)
		       if (feature[790] <= 0.0)
		        if (feature[908] <= -0.007)
		         predictor6= 1.0;
		        else if (feature[908] > -0.007)
		         if (feature[843] <= -0.001)
		          predictor6= 0.0;
		         else if (feature[843] > -0.001)
		          predictor6= 0.0;
		       else if (feature[790] > 0.0)
		        if (feature[725] <= 0.069)
		         if (feature[813] <= -0.073)
		          predictor6= 0.0;
		         else if (feature[813] > -0.073)
		          predictor6= 1.0;
		        else if (feature[725] > 0.069)
		         if (feature[835] <= -0.987)
		          predictor6= 0.0;
		         else if (feature[835] > -0.987)
		          predictor6= 1.0;
		      else if (feature[791] > 0.0)
		       if (feature[831] <= -0.48)
		        if (feature[358] <= 0.089)
		         if (feature[763] <= 0.079)
		          predictor6= 0.0;
		         else if (feature[763] > 0.079)
		          predictor6= 0.0;
		        else if (feature[358] > 0.089)
		         if (feature[743] <= -0.127)
		          predictor6= 1.0;
		         else if (feature[743] > -0.127)
		          predictor6= 0.0;
		       else if (feature[831] > -0.48)
		        if (feature[753] <= 0.026)
		         if (feature[741] <= 0.033)
		          predictor6= 1.0;
		         else if (feature[741] > 0.033)
		          predictor6= 1.0;
		        else if (feature[753] > 0.026)
		         if (feature[29] <= -0.081)
		          predictor6= 1.0;
		         else if (feature[29] > -0.081)
		          predictor6= 1.0;
		     else if (feature[811] > 0.0)
		      if (feature[5] <= -0.021)
		       if (feature[770] <= 0.0)
		        if (feature[830] <= -0.969)
		         predictor6= 0.0;
		        else if (feature[830] > -0.969)
		         predictor6= 1.0;
		       else if (feature[770] > 0.0)
		        if (feature[26] <= 0.147)
		         if (feature[836] <= -0.052)
		          predictor6= 0.0;
		         else if (feature[836] > -0.052)
		          predictor6= 1.0;
		        else if (feature[26] > 0.147)
		         if (feature[840] <= -0.987)
		          predictor6= 0.0;
		         else if (feature[840] > -0.987)
		          predictor6= 0.0;
		      else if (feature[5] > -0.021)
		       if (feature[0] <= 0.062)
		        if (feature[12] <= -0.002)
		         if (feature[54] <= 0.011)
		          predictor6= 0.0;
		         else if (feature[54] > 0.011)
		          predictor6= 1.0;
		        else if (feature[12] > -0.002)
		         if (feature[843] <= -0.416)
		          predictor6= 0.0;
		         else if (feature[843] > -0.416)
		          predictor6= 1.0;
		       else if (feature[0] > 0.062)
		        if (feature[832] <= -0.763)
		         predictor6= 0.0;
		        else if (feature[832] > -0.763)
		         predictor6= 1.0;
		 
		 // tree8
		 if (feature[795] <= -0.005)
		     if (feature[444] <= 0.02)
		      if (feature[836] <= -0.431)
		       if (feature[34] <= 0.057)
		        if (feature[66] <= 0.016)
		         if (feature[337] <= 0.019)
		          predictor7= 0.0;
		         else if (feature[337] > 0.019)
		          predictor7= 0.0;
		        else if (feature[66] > 0.016)
		         if (feature[0] <= 0.036)
		          predictor7= 1.0;
		         else if (feature[0] > 0.036)
		          predictor7= 0.0;
		       else if (feature[34] > 0.057)
		        if (feature[792] <= -0.056)
		         if (feature[5] <= -0.248)
		          predictor7= 0.0;
		         else if (feature[5] > -0.248)
		          predictor7= 1.0;
		        else if (feature[792] > -0.056)
		         predictor7= 0.0;
		      else if (feature[836] > -0.431)
		       if (feature[730] <= -0.157)
		        if (feature[99] <= -0.001)
		         predictor7= 0.0;
		        else if (feature[99] > -0.001)
		         if (feature[723] <= -0.28)
		          predictor7= 0.0;
		         else if (feature[723] > -0.28)
		          predictor7= 1.0;
		       else if (feature[730] > -0.157)
		        if (feature[766] <= 0.0)
		         predictor7= 1.0;
		        else if (feature[766] > 0.0)
		         if (feature[3] <= -0.016)
		          predictor7= 0.0;
		         else if (feature[3] > -0.016)
		          predictor7= 1.0;
		     else if (feature[444] > 0.02)
		      predictor7= 0.0;
		    else if (feature[795] > -0.005)
		     if (feature[811] <= 0.0)
		      if (feature[841] <= -0.005)
		       if (feature[745] <= -0.147)
		        if (feature[841] <= -0.306)
		         if (feature[878] <= 0.0)
		          predictor7= 0.0;
		         else if (feature[878] > 0.0)
		          predictor7= 0.0;
		        else if (feature[841] > -0.306)
		         if (feature[770] <= -0.001)
		          predictor7= 1.0;
		         else if (feature[770] > -0.001)
		          predictor7= 0.0;
		       else if (feature[745] > -0.147)
		        if (feature[750] <= 0.117)
		         if (feature[839] <= -0.709)
		          predictor7= 0.0;
		         else if (feature[839] > -0.709)
		          predictor7= 1.0;
		        else if (feature[750] > 0.117)
		         if (feature[7] <= 0.288)
		          predictor7= 0.0;
		         else if (feature[7] > 0.288)
		          predictor7= 1.0;
		      else if (feature[841] > -0.005)
		       if (feature[792] <= 0.0)
		        if (feature[752] <= -0.657)
		         if (feature[749] <= 0.062)
		          predictor7= 0.0;
		         else if (feature[749] > 0.062)
		          predictor7= 0.0;
		        else if (feature[752] > -0.657)
		         if (feature[744] <= -0.076)
		          predictor7= 1.0;
		         else if (feature[744] > -0.076)
		          predictor7= 1.0;
		       else if (feature[792] > 0.0)
		        if (feature[769] <= -0.014)
		         if (feature[743] <= -0.183)
		          predictor7= 1.0;
		         else if (feature[743] > -0.183)
		          predictor7= 1.0;
		        else if (feature[769] > -0.014)
		         if (feature[729] <= 0.032)
		          predictor7= 1.0;
		         else if (feature[729] > 0.032)
		          predictor7= 1.0;
		     else if (feature[811] > 0.0)
		      if (feature[12] <= 0.108)
		       if (feature[76] <= 0.1)
		        if (feature[835] <= -0.987)
		         if (feature[48] <= -0.065)
		          predictor7= 0.0;
		         else if (feature[48] > -0.065)
		          predictor7= 0.0;
		        else if (feature[835] > -0.987)
		         if (feature[742] <= -0.117)
		          predictor7= 1.0;
		         else if (feature[742] > -0.117)
		          predictor7= 1.0;
		       else if (feature[76] > 0.1)
		        if (feature[873] <= -0.292)
		         predictor7= 1.0;
		        else if (feature[873] > -0.292)
		         if (feature[852] <= -0.029)
		          predictor7= 0.0;
		         else if (feature[852] > -0.029)
		          predictor7= 1.0;
		      else if (feature[12] > 0.108)
		       if (feature[836] <= -0.431)
		        predictor7= 0.0;
		       else if (feature[836] > -0.431)
		        if (feature[810] <= 0.012)
		         predictor7= 1.0;
		        else if (feature[810] > 0.012)
		         if (feature[789] <= -0.001)
		          predictor7= 0.0;
		         else if (feature[789] > -0.001)
		          predictor7= 1.0;
		int[] finalpredictor;
		finalpredictor = new int[2];
		

		finalpredictor[0] = (int) predictor6;
		finalpredictor[1] = (int) predictor7;
		
		
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