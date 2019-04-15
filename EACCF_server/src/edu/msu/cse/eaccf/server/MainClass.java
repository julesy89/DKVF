package edu.msu.cse.eaccf.server;

import edu.msu.cse.dkvf.config.ConfigReader;

public class MainClass {
	public static void main(String args[]) {
		ConfigReader cnfReader = new ConfigReader(args[0]);
		EACCFServer gServer = new EACCFServer(cnfReader);
		gServer.runAll();
	}
}