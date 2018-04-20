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

public class RF2 extends BaseRichBolt{

	private OutputCollector collector;
	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		double predictor2 = 0;
		double predictor3 = 0;
		int rfid = 2;
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
		
		// Tree3
		if (feature[835] <= -0.987)
		     if (feature[395] <= -0.027)
		      if (feature[354] <= -0.001)
		       if (feature[350] <= 0.0)
		        if (feature[803] <= -0.003)
		         predictor2= 0.0;
		        else if (feature[803] > -0.003)
		         if (feature[875] <= -0.001)
		          predictor2= 1.0;
		         else if (feature[875] > -0.001)
		          predictor2= 0.0;
		       else if (feature[350] > 0.0)
		        predictor2= 1.0;
		      else if (feature[354] > -0.001)
		       if (feature[799] <= -0.101)
		        if (feature[754] <= 0.115)
		         if (feature[793] <= 0.043)
		          predictor2= 0.0;
		         else if (feature[793] > 0.043)
		          predictor2= 0.0;
		        else if (feature[754] > 0.115)
		         predictor2= 1.0;
		       else if (feature[799] > -0.101)
		        if (feature[372] <= -0.005)
		         predictor2= 1.0;
		        else if (feature[372] > -0.005)
		         if (feature[696] <= -0.002)
		          predictor2= 0.0;
		         else if (feature[696] > -0.002)
		          predictor2= 0.0;
		     else if (feature[395] > -0.027)
		      if (feature[818] <= 0.055)
		       if (feature[410] <= -0.005)
		        predictor2= 1.0;
		       else if (feature[410] > -0.005)
		        if (feature[281] <= -0.001)
		         if (feature[733] <= -0.096)
		          predictor2= 1.0;
		         else if (feature[733] > -0.096)
		          predictor2= 0.0;
		        else if (feature[281] > -0.001)
		         if (feature[849] <= 0.153)
		          predictor2= 0.0;
		         else if (feature[849] > 0.153)
		          predictor2= 0.0;
		      else if (feature[818] > 0.055)
		       if (feature[57] <= 0.001)
		        if (feature[751] <= 0.101)
		         if (feature[82] <= 0.155)
		          predictor2= 0.0;
		         else if (feature[82] > 0.155)
		          predictor2= 1.0;
		        else if (feature[751] > 0.101)
		         if (feature[157] <= 0.02)
		          predictor2= 0.0;
		         else if (feature[157] > 0.02)
		          predictor2= 1.0;
		       else if (feature[57] > 0.001)
		        if (feature[17] <= 0.0)
		         if (feature[733] <= -0.059)
		          predictor2= 0.0;
		         else if (feature[733] > -0.059)
		          predictor2= 0.0;
		        else if (feature[17] > 0.0)
		         if (feature[2] <= -0.052)
		          predictor2= 1.0;
		         else if (feature[2] > -0.052)
		          predictor2= 0.0;
		    else if (feature[835] > -0.987)
		     if (feature[827] <= -0.134)
		      if (feature[350] <= 0.001)
		       if (feature[819] <= -0.001)
		        if (feature[743] <= -0.003)
		         if (feature[784] <= -0.081)
		          predictor2= 1.0;
		         else if (feature[784] > -0.081)
		          predictor2= 0.0;
		        else if (feature[743] > -0.003)
		         if (feature[351] <= -0.001)
		          predictor2= 0.0;
		         else if (feature[351] > -0.001)
		          predictor2= 1.0;
		       else if (feature[819] > -0.001)
		        if (feature[805] <= 0.0)
		         if (feature[761] <= -0.122)
		          predictor2= 1.0;
		         else if (feature[761] > -0.122)
		          predictor2= 0.0;
		        else if (feature[805] > 0.0)
		         if (feature[68] <= -0.001)
		          predictor2= 0.0;
		         else if (feature[68] > -0.001)
		          predictor2= 1.0;
		      else if (feature[350] > 0.001)
		       if (feature[770] <= 0.007)
		        if (feature[843] <= -0.03)
		         if (feature[744] <= -0.07)
		          predictor2= 0.0;
		         else if (feature[744] > -0.07)
		          predictor2= 1.0;
		        else if (feature[843] > -0.03)
		         if (feature[715] <= 0.0)
		          predictor2= 1.0;
		         else if (feature[715] > 0.0)
		          predictor2= 0.0;
		       else if (feature[770] > 0.007)
		        if (feature[392] <= -0.001)
		         predictor2= 1.0;
		        else if (feature[392] > -0.001)
		         if (feature[684] <= -0.09)
		          predictor2= 0.0;
		         else if (feature[684] > -0.09)
		          predictor2= 1.0;
		     else if (feature[827] > -0.134)
		      if (feature[737] <= 0.15)
		       if (feature[781] <= -0.046)
		        if (feature[766] <= 0.0)
		         predictor2= 1.0;
		        else if (feature[766] > 0.0)
		         if (feature[815] <= -0.014)
		          predictor2= 1.0;
		         else if (feature[815] > -0.014)
		          predictor2= 1.0;
		       else if (feature[781] > -0.046)
		        if (feature[813] <= -0.134)
		         if (feature[376] <= 0.007)
		          predictor2= 1.0;
		         else if (feature[376] > 0.007)
		          predictor2= 0.0;
		        else if (feature[813] > -0.134)
		         if (feature[294] <= 0.05)
		          predictor2= 1.0;
		         else if (feature[294] > 0.05)
		          predictor2= 1.0;
		      else if (feature[737] > 0.15)
		       if (feature[266] <= -0.012)
		        predictor2= 1.0;
		       else if (feature[266] > -0.012)
		        if (feature[827] <= 0.0)
		         if (feature[158] <= -0.239)
		          predictor2= 1.0;
		         else if (feature[158] > -0.239)
		          predictor2= 0.0;
		        else if (feature[827] > 0.0)
		         predictor2= 1.0;
		
		// tree4
		if (feature[818] <= -0.003)
		     if (feature[76] <= 0.159)
		      if (feature[837] <= -0.974)
		       if (feature[689] <= 0.892)
		        if (feature[21] <= -0.229)
		         if (feature[780] <= -0.083)
		          predictor3= 0.0;
		         else if (feature[780] > -0.083)
		          predictor3= 0.0;
		        else if (feature[21] > -0.229)
		         if (feature[722] <= -0.037)
		          predictor3= 0.0;
		         else if (feature[722] > -0.037)
		          predictor3= 0.0;
		       else if (feature[689] > 0.892)
		        if (feature[793] <= 0.013)
		         predictor3= 0.0;
		        else if (feature[793] > 0.013)
		         if (feature[726] <= -0.085)
		          predictor3= 0.0;
		         else if (feature[726] > -0.085)
		          predictor3= 1.0;
		      else if (feature[837] > -0.974)
		       if (feature[743] <= -0.088)
		        if (feature[747] <= -0.048)
		         if (feature[771] <= 0.024)
		          predictor3= 1.0;
		         else if (feature[771] > 0.024)
		          predictor3= 1.0;
		        else if (feature[747] > -0.048)
		         if (feature[789] <= -0.081)
		          predictor3= 1.0;
		         else if (feature[789] > -0.081)
		          predictor3= 1.0;
		       else if (feature[743] > -0.088)
		        if (feature[767] <= -0.097)
		         predictor3= 0.0;
		        else if (feature[767] > -0.097)
		         if (feature[760] <= -0.09)
		          predictor3= 1.0;
		         else if (feature[760] > -0.09)
		          predictor3= 1.0;
		     else if (feature[76] > 0.159)
		      if (feature[756] <= -0.032)
		       if (feature[830] <= -0.969)
		        predictor3= 0.0;
		       else if (feature[830] > -0.969)
		        if (feature[872] <= 0.0)
		         if (feature[814] <= -0.011)
		          predictor3= 0.0;
		         else if (feature[814] > -0.011)
		          predictor3= 1.0;
		        else if (feature[872] > 0.0)
		         predictor3= 1.0;
		      else if (feature[756] > -0.032)
		       predictor3= 0.0;
		    else if (feature[818] > -0.003)
		     if (feature[905] <= -0.006)
		      predictor3= 1.0;
		     else if (feature[905] > -0.006)
		      if (feature[761] <= 0.033)
		       if (feature[840] <= -0.987)
		        if (feature[303] <= -0.013)
		         predictor3= 1.0;
		        else if (feature[303] > -0.013)
		         if (feature[767] <= -0.097)
		          predictor3= 0.0;
		         else if (feature[767] > -0.097)
		          predictor3= 0.0;
		       else if (feature[840] > -0.987)
		        if (feature[756] <= -0.02)
		         if (feature[747] <= -0.024)
		          predictor3= 0.0;
		         else if (feature[747] > -0.024)
		          predictor3= 1.0;
		        else if (feature[756] > -0.02)
		         if (feature[4] <= 0.074)
		          predictor3= 1.0;
		         else if (feature[4] > 0.074)
		          predictor3= 0.0;
		      else if (feature[761] > 0.033)
		       if (feature[750] <= 0.15)
		        if (feature[834] <= -0.332)
		         if (feature[501] <= -0.003)
		          predictor3= 0.0;
		         else if (feature[501] > -0.003)
		          predictor3= 0.0;
		        else if (feature[834] > -0.332)
		         if (feature[788] <= -0.015)
		          predictor3= 1.0;
		         else if (feature[788] > -0.015)
		          predictor3= 1.0;
		       else if (feature[750] > 0.15)
		        if (feature[213] <= 0.0)
		         if (feature[395] <= 0.0)
		          predictor3= 0.0;
		         else if (feature[395] > 0.0)
		          predictor3= 1.0;
		        else if (feature[213] > 0.0)
		         predictor3= 1.0;
		
		
		
		int[] finalpredictor;
		finalpredictor = new int[2];

		finalpredictor[0] = (int) predictor2;
		finalpredictor[1] = (int) predictor3;
		
		this.collector.emit(new Values (tuple.getValueByField("id"), finalpredictor,rfid));
		
		
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