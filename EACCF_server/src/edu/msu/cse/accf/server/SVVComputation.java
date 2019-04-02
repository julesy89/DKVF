package edu.msu.cse.accf.server;

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
		for(List<Integer> checkingGroups : server.checkingGroups) {

		    // for each server in the checking group
			for (int i : checkingGroups) {

                List<Long> _svv = svv.get(i);
				List<Long> _vv = server.vvs.get(i);

				for (int j = 0; j < server.trackingGroups.size(); j++) {
                    if (_svv.get(j) > _vv.get(j)) _svv.set(j, _vv.get(j));

				}

			}
		}

        server.setSvv(svv);


	}

}
