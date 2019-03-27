#!/bin/sh 
java -cp libs/*:EACCF_client.jar com.yahoo.ycsb.Client -t -db edu.msu.cse.dkvf.ycsbDriver.DKVFDriver -P properties.txt -p clientClassName=edu.msu.cse.accf.client.EACCFClient -p clientConfigFile=client_config  > exp_results/client_Client_0_original_t.result 2> exp_errors/client_Client_0_original_t.error