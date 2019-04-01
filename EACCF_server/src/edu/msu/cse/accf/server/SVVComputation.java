package edu.msu.cse.accf.server;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import edu.msu.cse.dkvf.metadata.Metadata.SVVMessage;
import edu.msu.cse.dkvf.metadata.Metadata.ServerMessage;
import edu.msu.cse.dkvf.metadata.Metadata.VVMessage;

public class SVVComputation implements Runnable {

	EACCFServer server;

	public SVVComputation(EACCFServer server) {
		this.server = server;
	}

	@Override
	public void run() {

        List<List<Long>> svv = new ArrayList<>();

		//take minimum of all childrens
		List<Long> minVv = new ArrayList<>();
		for (AtomicLong v : server.vv) {
			minVv.add(v.get());
		}

		for(List<Integer> checkingGroup : server.checkingGroups) {
			List<Long> _svv = new ArrayList<>(minVv);
			for (int serverInCheckingGroup : checkingGroup) {

				List<Long> vv = server.childrenVvs.get(serverInCheckingGroup);

				for (int i = 0; i < vv.size(); i++) {
					if (_svv.get(i) > vv.get(i)) {
						_svv.set(i, vv.get(i));
					}
				}
			}

			svv.add(_svv);

		}

		server.setSvv(svv);



		//if the node is parent it send DsvMessage to its children
		ServerMessage sm = null;
		if (server.parentPId == server.pId) {
			server.setSvv(minVv);
			sm = ServerMessage.newBuilder().setSvvMessage(SVVMessage.newBuilder().addAllSvvItem(minVv)).build();
			server.sendToAllInCheckingGroup(sm);
			//server.protocolLOGGER.info(MessageFormat.format(">>>SVV[0]= {0}", server.svv.get(0)));
		}
		//if the node is not root, it send vvMessage to its parent.
		else {
			VVMessage vvM = VVMessage.newBuilder().setPId(server.pId).addAllVvItem(minVv).build();
			sm = ServerMessage.newBuilder().setVvMessage(vvM).build();
			server.sendToServerViaChannel(server.cg_id + "_" + server.parentPId, sm);
		}

	}

}
