package edu.msu.cse.eaccf.server;

import edu.msu.cse.dkvf.ClientMessageAgent;
import edu.msu.cse.dkvf.DKVFServer;
import edu.msu.cse.dkvf.Storage.StorageStatus;
import edu.msu.cse.dkvf.config.ConfigReader;
import edu.msu.cse.dkvf.metadata.Metadata.*;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Experimental Adaptive Causal Consistency
 */
public class EACCFServer extends DKVFServer {

    // ----------------------------------------------------------------------------------------
    // Server Identification
    // ----------------------------------------------------------------------------------------

    // the replica id of this server
	int replicaID;

	// the partition id of this server
    int partitionID;

    // the id of this server as a string
    String serverID;

    // set of servers where the replicates are sent to
    Set<String> sendReplicate;


    // ----------------------------------------------------------------------------------------
    // TRACKING GROUP - (here: a server is assigned only to one tracking group)
    // ----------------------------------------------------------------------------------------

    // tracking group this server is in
    int trackingGroupID;

    // the VV vectors received in the tracking group - including my own VV
    Map<String, List<Long>> vvs = new HashMap<>();

    // list of servers the vv's are shared with (union of all tracking groups)
    Set<String> sendVV = new HashSet<>();

    // the tracking groups - in this experiment always all servers
    List<List<String>> trackingGroups = new ArrayList<>();


    // ----------------------------------------------------------------------------------------
    // CHECKING GROUP - (Static setup but each server can be assigned to multiple groups)
    // ----------------------------------------------------------------------------------------

    // svv matrix - for each checking group, we save the vv vectors
    List<List<Long>> svv;

    // the checking groups - the svv values of the checking groups make versions visible or not
    List<List<Integer>> checkingGroups = new ArrayList<>();


    // ----------------------------------------------------------------------------------------
    // Other Parameters for the Configuration
    // ----------------------------------------------------------------------------------------

    // interval to send the heartbeat messages
	int heartbeatInterval;

	// interval to send the svv message to all server in the checking group
	int svvComputationInterval;
	
	// the simulated message delay that is used
	int messageDelay;

	// variable to save the last replicate or heartbeat message
	long timeOfLastRepOrHeartbeat;

    // It is necessary to make sure that replicates are send FIFO
	Object putLock = new Object();


	public EACCFServer(ConfigReader cnfReader) {
		super(cnfReader);
		HashMap<String, List<String>> protocolProperties = cnfReader.getProtocolProperties();

		// read the configuration parameter and save them to the class
        replicaID = new Integer(protocolProperties.get("replicaID").get(0));
        partitionID = new Integer(protocolProperties.get("partitionID").get(0));
        trackingGroupID = new Integer(protocolProperties.get("trackingGroupID").get(0));
        serverID = replicaID + "_" + partitionID;

        protocolLOGGER.severe(String.format("Running server %s.", serverID));

        // set the replicates send all updates to
        sendReplicate = new HashSet(protocolProperties.get("replicas"));
        sendReplicate.remove(String.valueOf(replicaID));

        // parse all the checking groups and save them as a list of lists
        Utils.addEntriesAsMatrixAsString(String.valueOf(protocolProperties.get("tracking_groups"))
                .replace('[', ' ')
                .replace(']', ' '), trackingGroups);

        trackingGroups.get(trackingGroupID).forEach(c -> sendVV.add(c));
        sendVV.remove(serverID);

        // parse all the checking groups and save them as a list of lists
        Utils.addEntriesAsMatrix(String.valueOf(protocolProperties.get("checking_groups"))
                .replace('[', ' ')
                .replace(']', ' '), checkingGroups);

        // initialize all VVs for myself and all servers in the tracking group
        vvs.put(serverID, Utils.createList(trackingGroups.size(), 0L));
        trackingGroups.forEach(e -> e.forEach(c -> vvs.put(c, Utils.createList(trackingGroups.size(), Long.MAX_VALUE))));

        // initialize the svv matrix for all server in the checking group
        svv = Utils.createMatrix(checkingGroups.size(), trackingGroups.size(), Long.MAX_VALUE);

        // parse other attributes which are necessary for this server
        messageDelay = new Integer(protocolProperties.get("message_delay").get(0));
		heartbeatInterval = new Integer(protocolProperties.get("heartbeat_interval").get(0));
		svvComputationInterval = new Integer(protocolProperties.get("svv_comutation_interval").get(0));

		// Scheduling periodic operations
		ScheduledExecutorService heartbeatTimer = Executors.newScheduledThreadPool(1);
		ScheduledExecutorService dsvComputationTimer = Executors.newScheduledThreadPool(1);

		heartbeatTimer.scheduleAtFixedRate(new HeartbeatSender(this), 0, heartbeatInterval, TimeUnit.MILLISECONDS);
		dsvComputationTimer.scheduleAtFixedRate(new SVVComputation(this), 0, svvComputationInterval, TimeUnit.MILLISECONDS);
		
		protocolLOGGER.info("Server initiated successfully");
		channelDelay = messageDelay;
	}

    // ----------------------------------------------------------------------------------------
    // HANDLE CLIENT MESSAGES (GET, PUT)
    // ----------------------------------------------------------------------------------------


    public void handleClientMessage(ClientMessageAgent cma) {
		if (cma.getClientMessage().hasGetMessage()) {
			handleGetMessage(cma);
		} else if (cma.getClientMessage().hasPutMessage()) {
			handlePutMessage(cma);
		}

	}

	private void handleGetMessage(ClientMessageAgent cma) {
		protocolLOGGER.info(MessageFormat.format("Get message arrived! for key {0}", cma.getClientMessage().getGetMessage().getKey()));

        // get the actual message sent by the client
        GetMessage gm = cma.getClientMessage().getGetMessage();

        // the dependency set and checking group sent by the client
        List<TgTimeItem> ds = gm.getDsItemList();
        int cg = gm.getCg();

        // we have to wait because the client has more information then we have
		for (TgTimeItem dti : ds) {
			try {
                while(VV().get(dti.getTg()) < dti.getTime()) {
                    protocolLOGGER.info(MessageFormat.format("Waiting! vv[{0}] = {1} while ds[{0}]= {2}", dti.getTg(), VV().get(dti.getTg()), dti.getTime()));
                    //svv.wait();
                    Thread.sleep(1);
                }
			} catch (InterruptedException e) {
			    protocolLOGGER.severe("Interuption exception while waiting for consistent version");
			}	
		}

		// otherwise we have to get now the svv values according to the checking group
		List<Record> result = new ArrayList<>();
		boolean isSvvLargeEnough = true;
        for (TgTimeItem dti : ds) {
			if (svv.get(cg).get(dti.getTg()) < dti.getTime()) {
				isSvvLargeEnough = false;
				break;
			}
		}

        // define a predicate to make entries visible
        Predicate<Record> isVisible = (Record r) -> {
            if (svv.get(cg).get(r.getTg()) < r.getUt()) {
                return false;
            }
            for (TgTimeItem dti : r.getDsItemList()) {
                if (svv.get(cg).get(dti.getTg()) < dti.getTime()) {
                    protocolLOGGER.info("This version is not consistent, so I don't give to!");
                    return false;
                }
            }
            return true;
        };


		// depending on the svv check we make it visible or just returned new newest version locally used
		StorageStatus ss; 
		if (isSvvLargeEnough)
			ss = read(gm.getKey(), isVisible, result);
		else 
			ss = read(gm.getKey(), (Record r) -> true, result);

		// Prepare the client reply
		ClientReply cr;
		if (ss == StorageStatus.SUCCESS) {
			Record rec = result.get(0);
			cr = ClientReply.newBuilder()
                    .setStatus(true)
                    .setGetReply(
                            GetReply
                                    .newBuilder()
                                    .setD(rec))
                    .build();
		} else {
			cr = ClientReply.newBuilder()
                    .setStatus(false)
                    .build();
		}
		cma.sendReply(cr);
	}


	private void handlePutMessage(ClientMessageAgent cma) {

		PutMessage pm = cma.getClientMessage().getPutMessage();
		long dt = Utils.maxDsTime(pm.getDsItemList());
		updateHlc(dt);
		Record rec;

		synchronized (putLock) {

			rec = Record.newBuilder()
                    .setValue(pm.getValue())
                    .setUt(VV().get(trackingGroupID))
                    .setTg(trackingGroupID)
                    .addAllDsItem(pm.getDsItemList())
                    .build();

			sendReplicateMessages(pm.getKey(),rec); // The order is different than the paper
										// algorithm. We first send replicate to
										// insure a version with smaller
										// timestamp is replicated sooner.
		}
		StorageStatus ss = insert(pm.getKey(), rec);
		ClientReply cr;

		if (ss == StorageStatus.SUCCESS) {
			cr = ClientReply.newBuilder()
                    .setStatus(true)
                    .setPutReply(
                            PutReply
                                    .newBuilder()
                                    .setUt(rec.getUt())
                                    .setTg(trackingGroupID))
                    .build();

		} else {
			cr = ClientReply.newBuilder().setStatus(false).build();
		}
		cma.sendReply(cr);

	}



    // ----------------------------------------------------------------------------------------
    // HLC
    // ----------------------------------------------------------------------------------------

    private void updateHlc(long dt) {

        long vv_l = Utils.getL(VV().get(trackingGroupID));
        long physicalTime = Utils.getPhysicalTime();
        long dt_l = Utils.getL(dt);

        long newL = Math.max(Math.max(vv_l, dt_l), Utils.shiftToHighBits(physicalTime));

        long vv_c = Utils.getC(VV().get(trackingGroupID));
        long dt_c = Utils.getC(dt);
        long newC;
        if (newL == vv_l && newL == dt_l)
            newC = Math.max(vv_c, dt_c) + 1;
        else if (newL == vv_l)
            newC = vv_c + 1;
        else if (newL == dt_l)
            newC = dt_c + 1;
        else
            newC = 0;
        VV().set(trackingGroupID, newL + newC);

	}

	void updateHlc() {

        long vv_l = Utils.getL(VV().get(trackingGroupID));
        long physicalTime = Utils.getPhysicalTime();

        long newL = Math.max(vv_l, Utils.shiftToHighBits(physicalTime));

        long vv_c = Utils.getC(VV().get(trackingGroupID));
        long newC;
        if (newL == vv_l)
            newC = vv_c + 1;
        else
            newC = 0;
        VV().set(trackingGroupID, newL + newC);

	}


    // ----------------------------------------------------------------------------------------
    // HANDLE OR SEND SERVER MESSAGES (Heartbeat, Replicate, VV, SVV))
    // ----------------------------------------------------------------------------------------


    public void handleServerMessage(ServerMessage sm) {
        if (sm.hasReplicateMessage()) {
            handleReplicateMessage(sm);
        } else if (sm.hasHeartbeatMessage()) {
            handleHeartbeatMessage(sm);
        } else if (sm.hasVvMessage()) {
            handleVvMessage(sm);
        }
    }

    private void handleReplicateMessage(ServerMessage sm) {
		protocolLOGGER.finer(MessageFormat.format("Received replicate message: {0}", sm.toString()));
		int tg = sm.getReplicateMessage().getTg();
		Record d = sm.getReplicateMessage().getD();
		insert(sm.getReplicateMessage().getKey(), d);
		VV().set(tg, d.getUt());
	}

    private void sendReplicateMessages(String key, Record recordToReplicate) {
        ServerMessage sm = ServerMessage.newBuilder()
                .setReplicateMessage(
                        ReplicateMessage.newBuilder()
                                .setTg(trackingGroupID)
                                .setKey(key)
                                .setD(recordToReplicate))
                .build();

        for (String s : sendReplicate) {
            protocolLOGGER.finer(MessageFormat.format("Send replicate message to {0}: {1}", s, sm.toString()));
            sendToServerViaChannel(s + "_" + partitionID, sm);
        }
        //we don't need to synchronize for it, because it is not critical
        timeOfLastRepOrHeartbeat = Utils.getPhysicalTime();
    }

	void handleHeartbeatMessage(ServerMessage sm) {
		int tg = sm.getHeartbeatMessage().getTg();
		VV().set(tg, sm.getHeartbeatMessage().getTime());
	}

	void handleVvMessage(ServerMessage sm) {
        protocolLOGGER.finest("Receive" + sm.toString());
		int id = sm.getVvMessage().getPId();
		List<Long> _VV = sm.getVvMessage().getVvItemList();
		vvs.put(replicaID + "_" + id, _VV);
	}

    // ----------------------------------------------------------------------------------------
    // UTIL METHODS
    // ----------------------------------------------------------------------------------------

    void setSvv(List<List<Long>> svv) {
        synchronized (svv) {
            for (int i = 0; i < svv.size(); i++) {
                for (int j = 0; j < svv.get(i).size(); j++) {
                    this.svv.get(i).set(j, svv.get(i).get(j));
                }
            }
            svv.notify();
        }
    }


    List<Long> VV() {
	    return this.vvs.get(serverID);
    }




}
