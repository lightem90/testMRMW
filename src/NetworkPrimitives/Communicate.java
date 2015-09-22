package NetworkPrimitives;

import Structures.Message;
import Structures.Tag;
import Structures.View;
import com.robustMRMW.Node;
import EncoderDecoder.EncDec;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;

public class Communicate {

	//Class constants
	private static final int BUFFER_SIZE = 1024; // random for now
	private ByteBuffer readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
	private ByteBuffer writeBuffer = ByteBuffer.allocate(BUFFER_SIZE);


	//Class variables
	private int activeServers;
	private ArrayList<Boolean> turns;
	private ArrayList<SocketChannel> chan;

	//Custom objects
	private Tag lastTag;
	private Node n;
	private EncDec ED;
	private ConnectionManager caller;

	private enum Status {
		NOTACK, NOTSENT, ACK
	}

	//initialization
	public Communicate(Node currNode, ConnectionManager cm) {

		chan = cm.getServerChannels();
		activeServers = chan.size();


		//initializing all position to true, meaning we have to send to everyone
		turns = new ArrayList<>(activeServers);
		for (int i = 0; i < activeServers; i++) {

			turns.add(true);

		}

		n = currNode;
		ED = new EncDec();
		caller = cm;


	}

	/*
	 * query() starts and completes a "query" request to every other server
	 * returning the maximum tag value retrieved
	 */
	public Tag query() {

		Message request = new Message("query", n.getLocalTag(),
				n.getLocalView(), n.getMySett().getNodeId());

		Message[] values = waitForQuorum(request);

		//last tag CAN'T be smaller than local tag
		lastTag = findMaxTagFromMessages(values,n.getLocalTag());
		if (lastTag.compareTo(n.getLocalTag()) >= 0) //TODO: this check is useless, this condition is met in findMaxTagFromMessages
			return lastTag;
		return null;
	}

	/*
	 * preWrite() starts and completes a "pre-write" request to every other
	 * server returning true in case of success or false in case of failure
	 */
	public boolean preWrite(Tag newTag, View newView) {

		Message request = new Message("pre-write", newTag, newView, n.getMySett().getNodeId());
		lastTag = newTag;
		return !(waitForQuorum(request) == null);
	}

	/*
	 * finalizeRead() starts and completes a "finalize" request to every other
	 * server returning the View associated to the requested tag
	 */
	public View finalizeRead() {

		Message request = new Message("finalize", lastTag, n.getLocalView(),
				n.getMySett().getNodeId());

		Message[] values = waitForQuorum(request);
		int i = 0;
		while (values[i] == null)
			i++;
		return values[i].getView();
	}

	/*
	 * finalize() starts and completes a "finalize" request to every other
	 * server returning true in case of success or false in case of failure
	 */
	public boolean finalizeWrite() {

		Message request = new Message("finalize", lastTag, n.getLocalView(),
				n.getMySett().getNodeId());

		return !(waitForQuorum(request) == null);
	}
	private Message[] waitForQuorum(Message request) {

		Message reply;

		Message[] values = new Message[activeServers];
		ArrayList<Status> status = new ArrayList<>(activeServers);

		// initialize status and service variables, we have to send to everyone
		int ackCounter = 0;
		for (int i = 0; i < activeServers; i++) {
			values[i] = null;
			status.add(Status.NOTSENT);
		}

		//encoding message and writing it into the buffer
		String toSend = ED.encode(request);
		writeBuffer.clear();
		writeBuffer.put(toSend.getBytes());

		//writing message on EVERY open channel
		for (int i = 0; i < activeServers; i++) {

				try {
					System.out.println("Sending '"
							+ request.getRequestType()
							+ "' query to server");

					writeBuffer.flip();
					while (writeBuffer.hasRemaining())
						chan.get(i).write(writeBuffer);

				} catch (IOException e) {

					//if write fails it means that the channel has been closed so we cannot write to it;
					removeCrashedServer(i, chan);
					status.remove(i);
					i--;
					activeServers = caller.getServerChannels().size();
					System.out.println("Channel doesn't exist anymore. Active channels left:"+ activeServers);
					continue;
				}

				//this means that we sent the message to i and we are waiting for an ack from it
				turns.set(i,false);
				status.set(i, Status.NOTACK);

		}


			// Start receiving acks until quorum is reached.
			// In case of ack from a previous query, ignore the message and send the new query
			while (ackCounter < activeServers / 2 + 1) {

				for (int i = 0; i < activeServers; i++) {
					readBuffer.clear();
					String message = "";
					try {

						while (chan.get(i).read(readBuffer) > 0) {
							// flip the buffer to start reading
							readBuffer.flip();
							message += Charset.defaultCharset().decode(
									readBuffer);
						}

						if (message.equals(""))
							continue;
						//handling multiple messages on buffer
						String[] tokens = message.split("&");
						//decoding earlier message
						reply = ED.decode(tokens[0]);
					} catch (IOException e) {

						//if write fails it means that the channel has been closed so we cannot write to it
						removeCrashedServer(i, chan);
						status.remove(i);
						i--;
						activeServers = caller.getServerChannels().size();
						System.out.println("Channel doesn't exist anymore. Active channels left:"+ activeServers);
						continue;
					}

					//updating FD
					n.getFD().updateFDForNode(reply.getSenderId());


					//handling older messages
					if (status.get(i) == Status.NOTSENT) {

						try {
							System.out.println("Sending '"
									+ request.getRequestType()
									+ "' query to server (late) #" + reply.getSenderId());
							writeBuffer.flip();
							while (writeBuffer.hasRemaining()) {
								chan.get(i).write(writeBuffer);
							}

						} catch (IOException e) {

							//if write fails it means that the channel has been closed so we cannot write to it
							removeCrashedServer(i, chan);
							status.remove(i);
							i--;
							activeServers = caller.getServerChannels().size();
							System.out.println("Channel doesn't exist anymore. Active channels left:"+ activeServers);
							continue;
						}
						turns.set(i,false);
						status.set(i, Status.NOTACK);
					}
					//if it was not an older message we can store it consider the ack and wait for another write
					else {

						values[i] = new Message(reply.getRequestType(),reply.getTag(),reply.getView(),reply.getSenderId());
						status.set(i, Status.ACK);
						turns.set(i,true);
						ackCounter++;
					}
				}
			}

		//setTurns(turns);
		return values;

	}



	//utilities

	public static Tag findMaxTagFromMessages(Message[] values, Tag maxTag) {

		for (Message msg : values) {
			if (msg != null) {
				if (msg.getTag().compareTo(maxTag) > 0) {
					maxTag = msg.getTag();
				}
			}
		}

		return maxTag;
	}

	public void removeCrashedServer(int i, ArrayList<SocketChannel> serverChannels) {
		try {
			serverChannels.get(i).close();
			serverChannels.remove(i);
			turns.remove(i);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}


	/*Getters and Setters */
	public int getActiveServers() {
		return activeServers;
	}

	public void setActiveServers(int activeServers) {
		this.activeServers = activeServers;
	}

	public ArrayList<SocketChannel> getChan() {
		return chan;
	}

	public void setChan(ArrayList<SocketChannel> chan) {
		this.chan = chan;
	}
}
