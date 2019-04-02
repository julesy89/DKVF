package edu.msu.cse.accf.server;

import edu.msu.cse.dkvf.metadata.Metadata.SVVMessage;
import edu.msu.cse.dkvf.metadata.Metadata.ServerMessage;

import java.util.List;

import static edu.msu.cse.accf.server.Utils.createMatrix;

public class SVVComputation implements Runnable {

	EACCFServer server;

	public SVVComputation(EACCFServer server) {
	    this.server = server;
	}

	@Override
	public void run() {

        List<List<Long>> svv = createMatrix(server.checkingGroups.size(),
                server.trackingGroups.size(), Long.MAX_VALUE);

        // for each checking group we have defined
		for(List<Integer> cg : server.checkingGroups) {

		    // for each server in the checking group
			for (int id : cg) {

				List<Long> vv = server.vvs.get(id);
				for (int i = 0; i < vv.size(); i++) {

                    for (int j = 0; j < server.trackingGroups.size(); j++) {
                        if (svv.get(i).get(j) > vv.get(i)) svv.get(i).set(j, vv.get(i));
                    }

				}
			}

		}

		// just one vector because of tracking group assumption
		List<Long> _svv = svv.get(server.TRACKING_GROUP);
        server.setSvv(_svv);

        // send to all in checking group
        ServerMessage sm = ServerMessage.newBuilder().setSvvMessage(
                SVVMessage
                        .newBuilder()
                        .addAllSvvItem(_svv))
                .build();

        for (String s: server.sendSVV) {
            server.sendToServerViaChannel(s, sm);
        }

	}

}
