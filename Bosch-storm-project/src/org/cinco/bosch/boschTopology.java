package org.cinco.bosch;

import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.*;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;

public class boschTopology 
{
	private static final String TOPOLOGY_NAME = "Bosch-topology";
	public static void main(String args[]) throws InterruptedException
	{
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("SPOUT", new spout("/s/chopin/a/grad/akmittal/Downloads/stormsample.libsvm"));
		builder.setBolt("IDEXTRACTOR", new IdExtractor()).globalGrouping("SPOUT");
		builder.setBolt("RF1", new RF1()).allGrouping("IDEXTRACTOR");
		builder.setBolt("RF2", new RF2()).allGrouping("IDEXTRACTOR");
		builder.setBolt("RF3", new RF3()).allGrouping("IDEXTRACTOR");
		builder.setBolt("RF4", new RF4()).allGrouping("IDEXTRACTOR");
		builder.setBolt("RF5", new RF5()).allGrouping("IDEXTRACTOR");
		builder.setBolt("REPORTER", new Reporter()).allGrouping("RF1")
												  .allGrouping("RF2")
												  .allGrouping("RF3")
												   .allGrouping("RF4")
												   .allGrouping("RF5");
		
		Config config = new Config();
		config.setDebug(true);
		//LocalCluster cluster = new LocalCluster();
		try {
			StormSubmitter.submitTopology(TOPOLOGY_NAME,config,builder.createTopology()) ;
		} catch (AlreadyAliveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		
		} catch (AuthorizationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
	
