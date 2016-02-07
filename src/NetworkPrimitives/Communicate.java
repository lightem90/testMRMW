package NetworkPrimitives;

import Structures.*;
import com.robustMRMW.Node;
import EncoderDecoder.EncDec;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;

public class Communicate {

	enum Phase {
		QUERY, WRITE, FINALIZE, ANSWERING
	}


	//Class constants
	private static final int BUFFER_SIZE = 1024; // random for now
	private ByteBuffer readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
	private ByteBuffer writeBuffer = ByteBuffer.allocate(BUFFER_SIZE);
	private final static int INVALID = -1;
	private final static int PCE = 20;

	//Class variables
	private HashMap<Integer, Message> turnsValid;
	private HashMap<Integer, Message> turnsInvalid;
	private ArrayList<SocketChannel> chan;

	//Custom objects
	private Node n;
	private EncDec ED;
	private View toWrite;

	private int phaseId;
	private Phase c_phase;
	private Phase c_phaseNext;
	private Node.State c_state;

	private Tag maxTagInRead;


	//initialization
	public Communicate(Node currNode) {

		n = currNode;
		chan = currNode.getCm().getConnectedServerChannels();
		turnsValid = new HashMap<>();
		turnsInvalid = new HashMap<>();
		ED = new EncDec();
		System.out.println("Initialized communicate class");

	}


	//initializing write operation
	public void write(View V) {

		System.out.println("Initializing write procedure in communicate class");
		//Storing the view to write
		toWrite = V;
		//notifies the manager I'm writing
		c_state = Node.State.WRITING;
		n.getCm().setState(c_state);
		//Setting phases up
		c_phase = Phase.QUERY;
		c_phaseNext = Phase.WRITE;

		phaseId = getRndId();
		System.out.println("Query phase in write with id->"+phaseId);



		//sending query to everyone
		sendToEveryone(new Message(n.getSettings().getNodeId() + ConnectionManager.SEPARATOR + phaseId + ConnectionManager.SEPARATOR +"query",
				n.getLocalTag(),n.getLocalView(), n.getSettings().getNodeId(), n.getFD().getLeader_id()), true);

	}

	//initializing read operation
	public void read() {

		maxTagInRead = new Tag(	INVALID,INVALID,INVALID);
		//notifies the manager that I'm reading
		c_state = Node.State.READING;
		n.getCm().setState(c_state);
		//setting phases up
		c_phase = Phase.QUERY;
		c_phaseNext = Phase.FINALIZE;

		//phase identifier
		phaseId = getRndId();
		System.out.println("Query phase in read with id->"+phaseId);
		//first step is to send to everyone a query request
		sendToEveryone(new Message(n.getSettings().getNodeId() + ConnectionManager.SEPARATOR + phaseId + ConnectionManager.SEPARATOR +"query",
				n.getLocalTag(),n.getLocalView(), n.getSettings().getNodeId(),n.getFD().getLeader_id()), true);

	}


	public Node.State handleWriteMsg(Message rcv, String req) {

		String[] tokens = req.split(ConnectionManager.SEPARATOR);
		//First position: sender id,
		//Second: phaseID
		//Third: type

		boolean isStored = turnsValid.containsKey(rcv.getSenderId());
		if (isStored)
			//_ack message from this node already exists.. so probably is an old one o a very recent/fast one... don't know what to do with it
			return c_state;
		else {
			//Switch depending on the current phase
			switch (c_phase) {
				case QUERY:
					//if I'm querying and the received message is a query response, I don't have a message stored from that id, and the identifiers are correct
					if (tokens[2].equals("query") && Integer.parseInt(tokens[0]) == n.getSettings().getNodeId() && Integer.parseInt(tokens[1]) == phaseId) {

						System.out.println("Received expected message: " + req);
						turnsValid.put(rcv.getSenderId(), rcv);
						/* Updating the replica with the local node view */
						n.getCm().syncReplica(rcv,false);
						System.out.println("Total of query ack received: " + turnsValid.size());
						//If the quorum is reached step to next phase and clear turnsValid
						if (turnsValid.size() >= n.getSettings().getQuorum() - 1) {

							//Extracts the maximum tag comparing the local and the received ones
							Tag max = findMaxTagFromMessages(n.getLocalTag());

							phaseId = getRndId();
							//Sends a prewrite with a generated new tag
							sendToEveryone(new Message(n.getSettings().getNodeId() + ConnectionManager.SEPARATOR + phaseId + ConnectionManager.SEPARATOR + "pre-write",
									generateNewTag(max), toWrite, n.getSettings().getNodeId(),n.getFD().getLeader_id()), false);

							//this procedure is over so I step to pre-writing and I clear turnsValid map
							nextPhase();
							emptyMap(turnsValid);

							return c_state;

						} else
							return c_state;
					}
					//"not interesting" message
					break;

				case WRITE:
					//For writing I abort the pre-write if a quorum of invalid response arrive
					//if I'm querying and the received message is a query response and I don't have a message stored from that id
					if (tokens[2].equals("pre-write")  && Integer.parseInt(tokens[0]) == n.getSettings().getNodeId() && Integer.parseInt(tokens[1]) == phaseId) {
						if (!(rcv.getTag().getEpoch().getEpoch() == INVALID) && !(rcv.getTag().getEpoch().getId() == INVALID)) {
							//if it is a legit answer
							System.out.println("Received valid message: " + req);
							turnsValid.put(rcv.getSenderId(), rcv);
						}
						else {
							System.out.println("Received invalid message: " + req);
							turnsInvalid.put(rcv.getSenderId(), rcv);
						}
						//If the quorum is reached for valid response step to next phase and clear turnsValid
						if (turnsValid.size() >= n.getSettings().getQuorum() - 1) {

							nextPhase();
							emptyMap(turnsValid);
							emptyMap(turnsInvalid);

							phaseId = getRndId();
							//Sends a finalize message with the maximum tag (the nodes respond with it if its valid) and the view to finalize
							sendToEveryone(new Message(n.getSettings().getNodeId() + ConnectionManager.SEPARATOR + phaseId + ConnectionManager.SEPARATOR +"finalize" ,
									rcv.getTag(), n.getProposedView(), n.getSettings().getNodeId(),n.getFD().getLeader_id()), false);

							return c_state;

						} //else, if a quorum of valid answers is impossible
						else if (turnsInvalid.size() >= n.getSettings().getQuorum() - 1){

							//abort write: resetting phases and states
							abortWrite();
							return c_state;

						}
					}
					//"not interesting" message
					break;

				case FINALIZE:
					//if I'm querying and the received message is a query response and I don't have a message stored from that id
					if (tokens[2].equals("finalize") && Integer.parseInt(tokens[0]) == n.getSettings().getNodeId() && Integer.parseInt(tokens[1]) == phaseId) {
						turnsValid.put(rcv.getSenderId(), rcv);
						/* Updating the replica with the proposed node view */
						n.getCm().syncReplica(rcv,true);
						//If the quorum is reached step to next phase and clear turnsValid
						if (turnsValid.size() >= n.getSettings().getQuorum() - 1) {

							//I'm done writing
							toWrite.setLabel(View.Label.FIN);
							n.setLocalTag(rcv.getTag());
							n.setLocalView(toWrite);

							//If the write completes it continues with the leader election procedure (if it has to)
							switch (toWrite.getStatus()){
								case PROPOSE:
									toWrite.setStatus(Node.Status.INSTALL);
									n.getCm().write(toWrite);
									break;
								//TODO: checking if this is the right way to go
								case INSTALL:
									toWrite.setStatus(Node.Status.MULTICAST);
									n.getCm().setRnd(0);
									n.getCm().write(toWrite);
									break;

								//Do nothing
								case MULTICAST:
									int newRnd = n.getCm().getRnd();
									n.getCm().setRnd(newRnd);
								default:
									//case NONE or empty
									break;


							}

							nextPhase();
							emptyMap(turnsValid);

							return c_state;

						} else return c_state;
					}
					//"not interesting" message
					break;

			}

		}
	return c_state;

	}

	public Node.State handleReadMsg(Message rcv, String req) {

		String[] tokens = req.split(ConnectionManager.SEPARATOR);
		//First position: sender id,
		//Second: phaseID
		//Third: type

		boolean isStored = turnsValid.containsKey(rcv.getSenderId());
		if (isStored)
			//do nothing, probably was an old message
			return c_state;
		else {
			//Switch depending on the current phase
			switch (c_phase) {
				case QUERY:
					//if I'm querying and the received message is a query response, I don't have a message stored from that id, and the identifiers are correct
					if (tokens[2].equals("query") && Integer.parseInt(tokens[0]) == n.getSettings().getNodeId() && Integer.parseInt(tokens[1]) == phaseId) {

						System.out.println("Received expected message: " + req);
						turnsValid.put(rcv.getSenderId(), rcv);
						//If the quorum is reached step to next phase and clear turnsValid
						if (turnsValid.size() >= n.getSettings().getQuorum() - 1) {

							//Extracts the maximum tag comparing the local and the received ones
							maxTagInRead = findMaxTagFromMessages(n.getLocalTag());
							// I'm commenting this because I should not send a proper view, it must not be written
							//View rightView = findViewInMessages(max);

							phaseId = getRndId();
							//Sends finalize
							sendToEveryone(new Message( n.getSettings().getNodeId() + ConnectionManager.SEPARATOR + phaseId + ConnectionManager.SEPARATOR +"finalize" ,
									maxTagInRead, n.getProposedView(), n.getSettings().getNodeId(),n.getFD().getLeader_id()), false);

							//this procedure is over so I step to pre-writing and I clear turnsValid map
							nextPhase();
							emptyMap(turnsValid);

							return c_state;

						} else
							return c_state;
					}

				case FINALIZE:
					//if I'm querying and the received message is a query response and I don't have a message stored from that id
					if (tokens[2].equals("finalize") && Integer.parseInt(tokens[0]) == n.getSettings().getNodeId() && Integer.parseInt(tokens[1]) == phaseId) {

						System.out.println("Received expected message: " + req);
						turnsValid.put(rcv.getSenderId(), rcv);
						//If the quorum is reached I'm done reading
						if (turnsValid.size() >= n.getSettings().getQuorum() - 1) {

							//set last received tag (they should be all equals)
							//n.setLocalTag(rcv.getTag());
							//set the correct view CAN CAUSE TROUBLE BECAUSE I MAY SET AN EMPTY VIEW!
							//n.setLocalView(findViewInMessages(rcv.getTag()));

							System.out.println("Read complete");
							nextPhase();
							emptyMap(turnsValid);

							//gossiping
							sendToEveryone(new Message(n.getSettings().getNodeId() + ConnectionManager.SEPARATOR + 1 +ConnectionManager.SEPARATOR +"gossip" ,
									maxTagInRead, n.getLocalView(), n.getSettings().getNodeId(),n.getFD().getLeader_id()), false);

							return c_state;

						} else return c_state;
					}
			}

		}

		return c_state;

	}





	/*
	 * query() starts and completes a "query" request to every other server
	 * returning the maximum tag value retrieved
	 */


	private void sendToEveryone(Message m, boolean newOperation){


		UpdateAndCheckRndForPCE(m,newOperation);

		System.out.println("Sending '" + m.getRequestType() + "' to everyone");
		for (int i = 0; i < chan.size(); i++) {
			try {

				writeBuffer.clear();
				writeBuffer.put(ED.encode(m).getBytes());
				writeBuffer.flip();

				while (writeBuffer.hasRemaining()) {
					//if (chan.get(i).isConnectionPending() || !chan.get(i).isConnected())
					//	chan.get(i).finishConnect();
					chan.get(i).write(writeBuffer);
				}

			} catch (IOException e) {
				System.out.println("Cannot send message: " + m.getRequestType() + " to channel:" + chan.get(i).toString() + " in communicate");
				i--;
				continue;
			}

		}


	}

	private void UpdateAndCheckRndForPCE(Message msg, boolean newOp){

		if (newOp && msg.getView().getStatus() == Node.Status.MULTICAST && n.getSettings().getNodeId() == n.getFD().getLeader_id()) {
			int tmp = n.getCm().getRnd();
			n.getCm().setRnd(++tmp);
			//TODO: check if rnd == PCE
			/* if (tmp == PCE)
			* 	*/

		}
	}


	//utilities******************************************************************************************************

	//switching state I update phase and next phase. If it is done I updated state as well
	private void nextPhase() {

		switch (c_state) {
			case WRITING:
				//If I'm writing, the phase after query is writing
				if (c_phase == Phase.QUERY) {
					System.out.println("Phase " + c_phase + " completed. Next phase->" +c_phaseNext);
					c_phase = c_phaseNext;
					c_phaseNext = Phase.FINALIZE;
				}
				//If I'm writing, the phase after write is finalize
				else if (c_phase == Phase.WRITE) {
					System.out.println("Phase " + c_phase + " completed. Next phase->" +c_phaseNext);
					c_phase = c_phaseNext;
					c_phaseNext = Phase.ANSWERING;
				}
				//The successive phase and state of finalize is answering again, meaning I'm done with the writing procedure
				else if (c_phase == Phase.FINALIZE) {
					System.out.println("Phase " + c_phase + " completed. Next phase->" +c_phaseNext);
					c_phase = c_phaseNext;
					c_phaseNext = Phase.ANSWERING;
					c_state = Node.State.ANSWERING;
				}
				break;


			case READING:
				//If I'm reading, the phase after query is writing
				if (c_phase == Phase.QUERY) {
					System.out.println("Phase " + c_phase + " completed. Next phase->" +c_phaseNext);
					c_phase = c_phaseNext;
					c_phaseNext = Phase.WRITE;
				}
				//The successive phase and state of finalize in reading is answering again, meaning I'm done with the reading procedure after a gossip message
				else if (c_phase == Phase.FINALIZE) {
					System.out.println("Phase " + c_phase + " completed. Next phase->" +c_phaseNext);
					c_phase = c_phaseNext;
					c_phaseNext = Phase.ANSWERING;
					c_state = Node.State.ANSWERING;
				}
				break;
		}


	}

	//Answering again
	private void abortWrite(){

		c_state = Node.State.ANSWERING;
		n.getCm().setState(Node.State.ANSWERING);
		emptyMap(turnsInvalid);
		emptyMap(turnsValid);
		System.out.println("Write error, answering again");
	}

	private void emptyMap(HashMap<Integer,Message> map) {

//		//removing each element in turnsValid
//		Set<Integer> s = map.keySet();
//		for (int i : s)
//			map.remove(i);
		map.clear();

	}


	private int getRndId() {

		Random rand = new Random();
		int rnd = rand.nextInt(Integer.MAX_VALUE);
		System.out.println("New phase identifier->" +rnd);
		return rnd;

	}


	private Tag findMaxTagFromMessages(Tag local) {

		Tag maxTag = local;
		Collection<Message> replies = turnsValid.values();

		for (Message msg : replies) {
			if (msg != null) {
				if (msg.getTag().compareTo(local) > 0) {
					maxTag = msg.getTag();
				}
			}
		}
		//maxTag now is the biggest Tag we know about so I set it as local Tag
		//Is this right?
		n.setLocalTag(maxTag);
		return maxTag;
	}

	private View findViewInMessages(Tag t){

		Collection<Message> replies = turnsValid.values();

		for (Message msg : replies) {
			if (msg != null) {
				if (msg.getTag().compareTo(t) == 0)
					return msg.getView();
			}
		}
		return new View("");

	}

	/* This function takes old Tag and decide if adding an element to counter list or updating label (just +1 in our case) */
	private Tag generateNewTag(Tag lastTag) {

		int id = n.getSettings().getNodeId();
		Epoch ep = lastTag.getEpoch();
		//if counter is exhausted I start with a new label and set new counter to 0
		if (lastTag.isExhausted()) {
			ep.incrementEpoch();
			return new Tag(ep, 0);
		}

		//counter not exhausted so just adding the new value, first 2 lines should be useless
		lastTag.setEpoch(ep);
		int newCVal = (int) ((lastTag.getCounters().getFirst().getCounter())+1);
		lastTag.addCounter(new Counter(id,newCVal));

		System.out.println("New generated tag: epoch " + lastTag.getEpoch().getEpoch() + " id: " + lastTag.getEpoch().getId() +"-"+ lastTag.getCounters().getFirst().getId()
				+ " and counter: " +lastTag.getCounters().getFirst().getCounter() );

		return lastTag;
	}

	private void printAllCommunicateChannels() {

		System.out.println("Printing all communicate channels");
		for (SocketChannel c : chan)
			printRemoteAddressFromChannel(c);
		System.out.println("*************END**********");
	}

	private void printRemoteAddressFromChannel(SocketChannel s)
	{
		try {
			System.out.println("communicate channel: " + s.getRemoteAddress().toString());
		} catch (IOException e) {
			System.out.println("non-existing or yet not connected communicate channel");
		}

	}

	/*Getters and Setters */
	public ArrayList<SocketChannel> getChan() {
		return chan;
	}

	//Don't overwrite current channel list, just find the differences
	public void setChan(ArrayList<SocketChannel> channels) {
		//adding channel if not present
		for (SocketChannel c : channels) {
			if (!this.chan.contains(c)) {
				this.chan.add(c);
				System.out.print("Adding ");
				printRemoteAddressFromChannel(c);
			}

		}
		//deleting channel if wrong
		for (SocketChannel c : this.chan){
			if (!channels.contains(c)) {
				System.out.print("Removing ");
				this.chan.remove(c);
			}
		}
	}

}

