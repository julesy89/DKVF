package edu.msu.cse.eaccf.server;

import edu.msu.cse.dkvf.ServerListener;
import edu.msu.cse.dkvf.config.ConfigReader;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

public class MyMainClass {


    public static Logger frameworkLOGGER = Logger.getLogger("frameworkLogger");


	public static void main(String args[]) {

        final boolean LOAD_FROM_FILE = true;

        // the configuration to be passed to the server
        ConfigReader cnfReader;

        // whether we load from a file or not
	    if (LOAD_FROM_FILE) {
            cnfReader = new ConfigReader(args[0]);
        } else {
            cnfReader = new ConfigReader();

            HashMap<String, List<String>> prop = cnfReader.getProtocolProperties();

            prop.put("cg_id", Arrays.asList("0"));
            prop.put("tg_id", Arrays.asList("0"));
            prop.put("p_id", Arrays.asList("0"));
            prop.put("message_delay", Arrays.asList("0"));
            prop.put("parent_p_id", Arrays.asList("0"));
            prop.put("children_p_ids", Arrays.asList("0"));

            prop.put("num_of_tracking_groups", Arrays.asList("1"));
            prop.put("num_of_partitions", Arrays.asList("2"));
            prop.put("heartbeat_interval", Arrays.asList("1000"));
            prop.put("svv_comutation_interval", Arrays.asList("10"));
        }

		EACCFServer gServer = new EACCFServer(cnfReader);
        gServer.runAll();

        ServerListener sl = new ServerListener(new Integer(cnfReader.getConfig().getServerPort().trim()),
                gServer, MyMainClass.frameworkLOGGER);
        sl.run();

        //gServer.

        //gServer.runListeners();
	}
}