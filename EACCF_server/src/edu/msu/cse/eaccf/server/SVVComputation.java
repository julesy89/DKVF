package edu.msu.cse.eaccf.server;

import java.util.*;

public class SVVComputation implements Runnable {

	EACCFServer server;

	public SVVComputation(EACCFServer server) {
	    this.server = server;
	}

	@Override
	public void run() {

        List<List<Long>> svv = Utils.createMatrix(server.checkingGroups.size(),
                server.trackingGroups.size(), Long.MAX_VALUE);

        Map<String, List<Long>> _vvs = new HashMap<>(server.vvs);

        // for each checking group we have defined
        for (int k = 0; k < server.checkingGroups.size(); k++) {

            List<Integer> checkingGroup = server.checkingGroups.get(k);
            List<Long> _svv = svv.get(k);

            // for each server in the checking group
            for (int i : checkingGroup) {

                List<Long> _vv = _vvs.get(server.replicaID + "_" + i);

                for (int j = 0; j < server.trackingGroups.size(); j++) {
                    if (_svv.get(j) > _vv.get(j)) _svv.set(j, _vv.get(j));

                }

            }

		}

        server.setSvv(svv);


	}

}
