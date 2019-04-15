package edu.msu.cse.eaccf.client;

import edu.msu.cse.dkvf.config.ConfigReader;

import java.nio.charset.StandardCharsets;

public class MainClass {


	public static void main(String args[]) {

		ConfigReader cnfReader = new ConfigReader(args[0]);
        EACCFClient client = new EACCFClient(cnfReader);
        client.runAll();

        client.put("key1", "val1".getBytes());


        byte[] b = client.get("key");
        String test = new String(b, StandardCharsets.US_ASCII);
        System.out.println(test);


        //DKVFWorkload dkvfWorkload = new DKVFWorkload(null);

    }


}