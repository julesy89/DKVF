package edu.msu.cse.eaccf.client;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;

import edu.msu.cse.dkvf.DKVFClient;
import edu.msu.cse.dkvf.ServerConnector.NetworkStatus;
import edu.msu.cse.dkvf.Utils;
import edu.msu.cse.dkvf.config.ConfigReader;
import edu.msu.cse.dkvf.metadata.Metadata.ClientMessage;
import edu.msu.cse.dkvf.metadata.Metadata.ClientReply;
import edu.msu.cse.dkvf.metadata.Metadata.GetMessage;
import edu.msu.cse.dkvf.metadata.Metadata.PutMessage;
import edu.msu.cse.dkvf.metadata.Metadata.TgTimeItem;

public class EACCFClient extends DKVFClient {

    // dependency set that contains for each tracking group one value
	HashMap<Integer, Long> ds;

    // the replicate which the client is only speaking to
	int replicaID;

	// So fare hardcoded checking group - needs to change dynamically later on
	final int CHECKING_GROUP = 0;

	// number of partitions (necessary for key - server mapping here)
	int numOfPartitions;


	public EACCFClient(ConfigReader cnfReader) {
		super(cnfReader);
		HashMap<String, List<String>> protocolProperties = cnfReader.getProtocolProperties();
		numOfPartitions = new Integer(protocolProperties.get("num_of_partitions").get(0));
		replicaID = new Integer(protocolProperties.get("replicaID").get(0));
		ds = new HashMap<>();
	}


	public boolean put(String key, byte[] value) {
		try {
			PutMessage pm = PutMessage
                    .newBuilder()
                    .setKey(key)
                    .setValue(ByteString.copyFrom(value))
                    .addAllDsItem(getTgTimeItems())
                    .build();

			ClientMessage cm = ClientMessage.newBuilder().setPutMessage(pm)
                    .build();

			String serverId = findServer (key);

			if (sendToServer(serverId, cm) == NetworkStatus.FAILURE)
				return false;
			ClientReply cr = readFromServer(serverId);

			if (cr != null && cr.getStatus()) {
				updateDS(cr.getPutReply().getTg(), cr.getPutReply().getUt());
				return true;
			} else {
				protocolLOGGER.severe("Server could not put the key= " + key);
				return false;
			}
		} catch (Exception e) {
			protocolLOGGER.severe(Utils.exceptionLogMessge("Failed to put due to exception", e));
			return false;
		}
	}

	public byte[] get(String key) {
		try {
			GetMessage gm = GetMessage
                    .newBuilder()
                    .addAllDsItem(getTgTimeItems())
                    .setKey(key)
                    .setCg(CHECKING_GROUP)
                    .build();

			ClientMessage cm = ClientMessage
                    .newBuilder().
                            setGetMessage(gm).build();

			String serverId = findServer (key);

			if (sendToServer(serverId, cm) == NetworkStatus.FAILURE)
				return null;
			ClientReply cr = readFromServer(serverId);
			if (cr != null && cr.getStatus()) {
				for (TgTimeItem tti : cr.getGetReply().getD().getDsItemList()) {
					updateDS(tti.getTg(), tti.getTime());
				}
				updateDS(cr.getGetReply().getD().getTg(), cr.getGetReply().getD().getUt());
				return cr.getGetReply().getD().getValue().toByteArray();
			} else {
				protocolLOGGER.severe("Server could not get the key= " + key);
				return null;
			}
		} catch (Exception e) {
			protocolLOGGER.severe(Utils.exceptionLogMessge("Failed to get due to exception", e));
			return null;
		}
	}

	public String findServer(String key) throws NoSuchAlgorithmException {

		//long hash = Utils.getMd5HashLong(key);
		//int partition =  (int) (hash % numOfPartitions);

		int partition = 0;

		return replicaID + "_" + partition; //we should change this function for final system. It can query a load balancer to find the server.
	}

	private void updateDS(int tg, long time) {
		if (ds.containsKey(tg))
			ds.put(tg, Math.max(time, ds.get(tg)));
		else {
			ds.put(tg, time);
		}
	}

	private List<TgTimeItem> getTgTimeItems() {
		List<TgTimeItem> result = new ArrayList<>();
		for (Map.Entry<Integer, Long> entry : ds.entrySet()) {

			TgTimeItem dti = TgTimeItem
					.newBuilder()
                    .setTg(entry.getKey())
                    .setTime(entry.getValue())
                    .build();

			result.add(dti);
		}
		return result;
	}
}
