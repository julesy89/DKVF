package edu.msu.cse.eaccf.server;

import edu.msu.cse.dkvf.metadata.Metadata.HeartbeatMessage;
import edu.msu.cse.dkvf.metadata.Metadata.ServerMessage;

public class HeartbeatSender implements Runnable {
	EACCFServer server;
	public HeartbeatSender(EACCFServer server) {
		this.server = server;
	}
	@Override
	public void run() {
		long ct = System.currentTimeMillis(); 
		if (ct > server.timeOfLastRepOrHeartbeat + server.heartbeatInterval){
			server.updateHlc();
			ServerMessage sm = ServerMessage.newBuilder()
					.setHeartbeatMessage(
							HeartbeatMessage
									.newBuilder()
									.setTg(server.trackingGroupID)
									.setTime(server.VV().get(server.trackingGroupID)))
					.build();

			for (String s : server.sendReplicate) {
				server.sendToServerViaChannel(s + "_" + server.partitionID, sm);
			}

			server.timeOfLastRepOrHeartbeat = Utils.getPhysicalTime();
		}

	}
}
