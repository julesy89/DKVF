package edu.msu.cse.accf.server;

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
			ServerMessage sm = ServerMessage.newBuilder().setHeartbeatMessage(HeartbeatMessage.newBuilder().setTg(server.tg_id).setTime(server.vv.get(server.tg_id).get())).build();
			for (String id : server.sendVV) {
				server.sendToServerViaChannel(id, sm);
			}
			server.timeOfLastRepOrHeartbeat = Utils.getPhysicalTime();
		}

	}
}