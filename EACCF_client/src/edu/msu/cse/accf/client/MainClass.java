package edu.msu.cse.accf.client;

import edu.msu.cse.dkvf.config.ConfigReader;

public class MainClass {


	public static void main(String args[]) {

		ConfigReader cnfReader = new ConfigReader(args[0]);
        EACCFClient client = new EACCFClient(cnfReader);

        client.runAll();
        client.put("key", "val".getBytes());

    }


}