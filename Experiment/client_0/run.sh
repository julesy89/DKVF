java -cp libs/*:EACCF_client.jar com.yahoo.ycsb.Client -t -db edu.msu.cse.dkvf.ycsbDriver.DKVFDriver -P properties.txt -p clientClassName=edu.msu.cse.eaccf.client.EACCFClient -p clientConfigFile=$1
