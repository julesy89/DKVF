package edu.msu.cse.accf.server;

import edu.msu.cse.dkvf.ClientMessageAgent;
import edu.msu.cse.dkvf.DKVFServer;
import edu.msu.cse.dkvf.Storage.StorageStatus;
import edu.msu.cse.dkvf.config.ConfigReader;
import edu.msu.cse.dkvf.metadata.Metadata.*;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

public class EACCFServer extends DKVFServer {

	List<List<Long>> svv;

	int replicaID;
    int partitionID;

	ArrayList<AtomicLong> vv;

	List<List<Integer>> checkingGroups;
    Set<Integer> sendSVV;

    List<List<String>> trackingGroups;
    Set<String> sendVV;

    HashMap<Integer, List<Long>> childrenVvs;

	int parentPId;

	// intervals
	int heartbeatInterval;
	int svvComutationInterval;

	
	//simulation parameters
	int messageDelay;  
	
	// Heartbeat
	long timeOfLastRepOrHeartbeat;

	Object putLock = new Object(); // It is necessary to make sure that
									// replicates are send
	// FIFO

	public EACCFServer(ConfigReader cnfReader) {
		super(cnfReader);
		HashMap<String, List<String>> protocolProperties = cnfReader.getProtocolProperties();

        replicaID = new Integer(protocolProperties.get("replicaID").get(0));
        partitionID = new Integer(protocolProperties.get("partitionID").get(0));
		messageDelay = new Integer(protocolProperties.get("message_delay").get(0));


        trackingGroups = new ArrayList<>();
        String str = String.valueOf(protocolProperties.get("tracking_groups"));
        String[] vals = str.split(";");
        for (String entry : vals) {
            List<String> l = new ArrayList<>();
            for (String e : entry.split(",")){
                l.add(e);
                sendVV.add(e);
            }
            trackingGroups.add(l);
        }

        vv = new ArrayList<>();
        ArrayList<Long> allZero = new ArrayList<>();
        for (int i = 0; i < trackingGroups.size(); i++) {
            vv.add(i, new AtomicLong(0));
            allZero.add(new Long(0));
        }


        checkingGroups = new ArrayList<>();
        str = String.valueOf(protocolProperties.get("checking_groups"));
        vals = str.split(";");
        for (String entry : vals) {
            List<Integer> l = new ArrayList<>();
            for (String e : entry.split(",")){
                int i = Integer.valueOf(e);
                l.add(i);
                sendSVV.add(i);
            }
            checkingGroups.add(l);
        }

        svv = new ArrayList<>();
        for (int i = 0; i < checkingGroups.size(); i++) {
            List<Long> l = new ArrayList<>();
            for (int j = 0; j < trackingGroups.size(); j++) {
                l.add(new Long(0));
            }
            svv.add(l);
        }


		heartbeatInterval = new Integer(protocolProperties.get("heartbeat_interval").get(0));
		svvComutationInterval = new Integer(protocolProperties.get("svv_comutation_interval").get(0));


		// Scheduling periodic operations
		ScheduledExecutorService heartbeatTimer = Executors.newScheduledThreadPool(1);
		ScheduledExecutorService dsvComputationTimer = Executors.newScheduledThreadPool(1);

		heartbeatTimer.scheduleAtFixedRate(new HeartbeatSender(this), 0, heartbeatInterval, TimeUnit.MILLISECONDS);
		dsvComputationTimer.scheduleAtFixedRate(new SVVComputation(this), 0, svvComutationInterval, TimeUnit.MILLISECONDS);
		
		protocolLOGGER.info("Server initiated successfully");
		channelDelay = messageDelay;
	}

	public void handleClientMessage(ClientMessageAgent cma) {
		if (cma.getClientMessage().hasGetMessage()) {
			handleGetMessage(cma);
		} else if (cma.getClientMessage().hasPutMessage()) {
			handlePutMessage(cma);
		}

	}

	private void handleGetMessage(ClientMessageAgent cma) {
		protocolLOGGER.info(MessageFormat.format("Get message arrived! for key {0}", cma.getClientMessage().getGetMessage().getKey()));
		// In this implementation, we assume the server is in only one checking group.
		// Thus, we don't read client's checking group for now.
		// wait for SVV

		List<TgTimeItem> ds = cma.getClientMessage().getGetMessage().getDsItemList();
		for (int i = 0; i < ds.size(); i++) {
			TgTimeItem dti = ds.get(i);
			try {
			   // synchronized(svv) {
			        while(vv.get(dti.getTg()).get() < dti.getTime()) {
			        	protocolLOGGER.info(MessageFormat.format("Waiting! vv[{0}] = {1} while ds[{0}]= {2}", dti.getTg(), vv.get(dti.getTg()).get(), dti.getTime()));
			        	//svv.wait();
			        	Thread.sleep(1);
			        }
			   // }
			} catch (InterruptedException e) {
			    protocolLOGGER.severe("Intruption exception while waiting for consistent version");
			}	
		}
		GetMessage gm = cma.getClientMessage().getGetMessage();
		List<Record> result = new ArrayList<>();
		boolean isSvvLargeEnough = true; 
		for (int i = 0; i < ds.size(); i++) {
			TgTimeItem dti = ds.get(i);
			if (svv.get(gm.getCg()).get(dti.getTg()) < dti.getTime()) {
				isSvvLargeEnough = false;
				break;
			}
		}
		
		StorageStatus ss; 
		if (isSvvLargeEnough)
			ss = read(gm.getKey(), isVisible, result);
		else 
			ss = read(gm.getKey(), (Record r) -> {return true;}, result);
		ClientReply cr = null;
		if (ss == StorageStatus.SUCCESS) {
			Record rec = result.get(0);
			//protocolLOGGER.info(MessageFormat.format("number of ds items= {0}",  result.get(0).getDsItemCount()));
			//TgTimeItem dti = rec.getDsItem(0);
			//protocolLOGGER.info(MessageFormat.format("svv[{0}]={1}, d.ds[{0}] = {2}", dti.getTg(), svv.get(dti.getTg()), dti.getTime()));
			//protocolLOGGER.info(MessageFormat.format("myVV[{0}]={1}, childVV[{0}] = {2}", dti.getTg(), vv.get(dti.getTg()), childrenVvs.get(1).get(0)));
			
			cr = ClientReply.newBuilder().setStatus(true).setGetReply(GetReply.newBuilder().setD(rec)).build();
		} else {
			cr = ClientReply.newBuilder().setStatus(false).build();
		}
		cma.sendReply(cr);
	}


	Predicate<Record> isVisible = (Record r) -> {

		//protocolLOGGER.info(MessageFormat.format("number of ds items= {0}",  r.getDsItemCount()));
		if (svv.get(r.getTg()) < r.getUt()) {
			return false;
		}
		for (int i = 0; i < r.getDsItemCount(); i++) {
			TgTimeItem dti = r.getDsItem(i);
			//protocolLOGGER.info(MessageFormat.format("ssv[{0}]={1}, d.ds[{0}] = {2}", dti.getTg(), svv.get(dti.getTg()), dti.getTime()));
			if (svv.get(dti.getTg()) < dti.getTime()) {
				protocolLOGGER.info("This version is not consistent, so I don't give to!");
				return false;
				
			}
				
		}
		return true;
	};

	private void handlePutMessage(ClientMessageAgent cma) {

		PutMessage pm = cma.getClientMessage().getPutMessage();
		long dt = Utils.maxDsTime(pm.getDsItemList());
		updateHlc(dt);
		Record rec = null;
		synchronized (putLock) {
			rec = Record.newBuilder().setValue(pm.getValue()).setUt(vv.get(tg_id).get()).setTg(tg_id).addAllDsItem(pm.getDsItemList()).build();
			sendReplicateMessages(pm.getKey(),rec); // The order is different than the paper
										// algorithm. We first send replicate to
										// insure a version with smaller
										// timestamp is replicated sooner.
		}
		StorageStatus ss = insert(pm.getKey(), rec);
		ClientReply cr = null;

		if (ss == StorageStatus.SUCCESS) {
			cr = ClientReply.newBuilder().setStatus(true).setPutReply(PutReply.newBuilder().setUt(rec.getUt()).setTg(tg_id)).build();
		} else {
			cr = ClientReply.newBuilder().setStatus(false).build();
		}
		cma.sendReply(cr);
	}

	private void sendReplicateMessages(String key, Record recordToReplicate) {
		ServerMessage sm = ServerMessage.newBuilder().setReplicateMessage(ReplicateMessage.newBuilder().setTg(tg_id).setKey(key).setD(recordToReplicate)).build();

		for (String s : sendVV) { //We can implement different data placement policies here.
			protocolLOGGER.finer(MessageFormat.format("Send replicate message to {0}: {1}", s, sm.toString()));
			sendToServerViaChannel(s, sm);
		}
		timeOfLastRepOrHeartbeat = Utils.getPhysicalTime(); //we don't need to synchronize for it, because it is not critical
	}

	private void updateHlc(long dt) {

        for (int tg_id = 0; tg_id < trackingGroups.size(); tg_id++) {

            long vv_l = Utils.getL(vv.get(tg_id).get());
            long physicalTime = Utils.getPhysicalTime();
            long dt_l = Utils.getL(dt);

            long newL = Math.max(Math.max(vv_l, dt_l), Utils.shiftToHighBits(physicalTime));

            long vv_c = Utils.getC(vv.get(tg_id).get());
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
            vv.get(tg_id).set(newL + newC);

        }
	}

	void updateHlc() {

        for (int tg_id = 0; tg_id < trackingGroups.size(); tg_id++) {

            long vv_l = Utils.getL(vv.get(tg_id).get());
            long physicalTime = Utils.getPhysicalTime();

            long newL = Math.max(vv_l, Utils.shiftToHighBits(physicalTime));

            long vv_c = Utils.getC(vv.get(tg_id).get());
            long newC;
            if (newL == vv_l)
                newC = vv_c + 1;
            else
                newC = 0;
            vv.get(tg_id).set(newL + newC);
        }
	}

	public void handleServerMessage(ServerMessage sm) {
		if (sm.hasReplicateMessage()) {
			handleReplicateMessage(sm);
		} else if (sm.hasHeartbeatMessage()) {
			handleHearbeatMessage(sm);
		} else if (sm.hasVvMessage()) {
			handleVvMessage(sm);
		} else if (sm.hasSvvMessage()) {
			handleSvvMessage(sm);
		}
	}

	private void handleReplicateMessage(ServerMessage sm) {
		protocolLOGGER.finer(MessageFormat.format("Received replicate message: {0}", sm.toString()));
		int senderTgId = sm.getReplicateMessage().getTg();
		Record d = sm.getReplicateMessage().getD();
		insert(sm.getReplicateMessage().getKey(), d);
		vv.get(senderTgId).set(d.getUt());
	}

	void handleHearbeatMessage(ServerMessage sm) {
		int senderTgId = sm.getHeartbeatMessage().getTg();
		vv.get(senderTgId).set(sm.getHeartbeatMessage().getTime());
	}

	void handleVvMessage(ServerMessage sm) {
		int senderPId = sm.getVvMessage().getPId();
		List<Long> receivedVv = sm.getVvMessage().getVvItemList();
		protocolLOGGER.finest("Receive" + sm.toString());
		childrenVvs.put(senderPId, receivedVv);
	}

	void handleSvvMessage(ServerMessage sm) {
		protocolLOGGER.finest(sm.toString());
		setSvv(sm.getSvvMessage().getSvvItemList());

        // we do not want to change the meta message data here
		List<Long> concatenateSVV = new ArrayList<>();
		for (List<Long> _svv : svv) concatenateSVV.addAll(_svv);

		sm = ServerMessage.newBuilder().setSvvMessage(SVVMessage.newBuilder().addAllSvvItem(concatenateSVV)).build();
		sendToAllInCheckingGroup(sm);
	}

	void sendToAllInCheckingGroup(ServerMessage sm) {
		for (int partition : sendSVV) {
			sendToServerViaChannel(replicaID + "_" + partition, sm);
		}
	}

	void setSvv(List<Long> newSvv) {
		synchronized (svv) {
			for (int i=0; i<newSvv.size();i++)
				svv.set(i, newSvv.get(i));	
			svv.notify();
		}
	}
}
