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
	public static final int BUFFER_SIZE = 1024; // random for now
	ByteBuffer readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
	ByteBuffer writeBuffer = ByteBuffer.allocate(BUFFER_SIZE);

	//Class variables
	int serverCount;
	ArrayList<Boolean> turns;
	ArrayList<SocketChannel> chan;

	//Custom objects
	Tag lastTag;
	Node n;
	EncDec ED;
	ConnectionManager caller;

	public enum Status {
		NOTACK, NOTSENT, ACK
	}

	public Communicate(Node currNode, ConnectionManager cm) {

		serverCount = cm.getServerCount();
		chan = cm.getServerChannels();

		//initializing all position to true
		turns = new ArrayList<>(serverCount);
		for (int i = 0; i < serverCount; i++) {

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

		lastTag = findMaxTagFromMessages(values);
		if (lastTag.compareTo(n.getLocalTag()) >= 0)
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

	// TODO: if I remove a channel at line 121, it happens that I may request the same index at line 144 and 188 causing an array out of bound exception I have no idea how to fix it
	private Message[] waitForQuorum(Message request) {

		Message reply;

		Message[] values = new Message[serverCount];
		ArrayList<Status> status = new ArrayList<>(serverCount);

		// initialize status and service variables
		int ackCounter = 0;
		for (int i = 0; i < serverCount; i++) {
			values[i] = null;
			status.add(Status.NOTSENT);
		}

		String toSend = ED.encode(request);
		writeBuffer.clear();
		writeBuffer.put(toSend.getBytes());

		// if it is my turn, send the message to server i, but i should send to everyone right? (old version is commented)

//OLD VERSION
/*			for (int i = 0; i < serverCount; i++) {

				if (turns.get(i)) {
					try {
						System.out.println("Sending '"
								+ request.getRequestType()
								+ "' query to server");

						writeBuffer.flip();
						while (writeBuffer.hasRemaining())
							chan.get(i).write(writeBuffer);

					} catch (IOException e) {
						System.out
								.println("Channel doesn't exist anymore, removing it");
						n.removeCrashedServer(i);
						status.remove(i);
						i--;
						serverCount = n.getServerCount();
						continue;
					}

					turns.set(i,false);
					status.set(i, Status.NOTACK);

				}
			}
*/

		//NEW VERSION: SENDING TO EVERYONE BUT WAITING ONLY FOR QUORUM

		for (int i = 0; i < serverCount; i++) {

				try {
					System.out.println("Sending '"
							+ request.getRequestType()
							+ "' query to server");

					writeBuffer.flip();
					while (writeBuffer.hasRemaining())
						chan.get(i).write(writeBuffer);

				} catch (IOException e) {
					System.out
							.println("Channel doesn't exist anymore, removing it");
					removeCrashedServer(i, chan);
					status.remove(i);
					i--;
					serverCount--;
					continue;
				}

				turns.set(i,false);
				status.set(i, Status.NOTACK);

		}


			// Start receiving acks until quorum is reached.
			// In case of ack from a previous query, ignore the message and send the new query
			while (ackCounter < serverCount / 2 + 1) {

				for (int i = 0; i < serverCount; i++) {
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
						String[] tokens = message.split("&");
						reply = ED.decode(tokens[0]);
					} catch (IOException e) {
						System.out
								.println("Channel doesn't exist anymore, removing it");
						removeCrashedServer(i, chan);
						status.remove(i);
						i--;
						serverCount--;
						continue;
					}


					/* I update FD only on the "receiver" side when I answer back
					int sendID = reply.getSenderId();
					caller.getFD().updateFDForNode(sendID);
					*/



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
							System.out
									.println("Channel doesn't exist anymore, removing it");
							removeCrashedServer(i, chan);
							status.remove(i);
							serverCount--;
							i--;
							continue;
						}
						turns.set(i,false);
						status.set(i, Status.NOTACK);
					} else {
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

	public static Tag findMaxTagFromMessages(Message[] values) {
		Tag maxTag = new Tag(0, 0);
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
			serverCount--;
			turns.remove(i);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
