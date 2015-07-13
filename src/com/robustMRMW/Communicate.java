package com.robustMRMW;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;

public class Communicate {

	Tag lastTag;
	Node n;

	Communicate(Node n) {
		this.n = n;
	}

	public enum Status {
		NOTACK, NOTSENT, ACK
	}

	/*
	 * query() starts and completes a "query" request to every other server
	 * returning the maximum tag value retrieved
	 */
	public Tag query() {

		Message request = new Message("query", n.getLocalTag(),
				n.getLocalView(), n.getId());

		Message[] values = waitForQuorum(request);

		lastTag = Node.findMaxTagFromMessages(values);
		if (lastTag.compareTo(n.getLocalTag()) >= 0)
			return lastTag;
		return null;
	}

	/*
	 * preWrite() starts and completes a "pre-write" request to every other
	 * server returning true in case of success or false in case of failure
	 */
	public boolean preWrite(Tag newTag, View newView) {

		Message request = new Message("pre-write", newTag, newView, n.getId());
		lastTag = newTag;
		return !(waitForQuorum(request) == null);
	}

	/*
	 * finalizeRead() starts and completes a "finalize" request to every other
	 * server returning the View associated to the requested tag
	 */
	public View finalizeRead() {

		Message request = new Message("finalize", lastTag, n.getLocalView(),
				n.getId());

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
				n.getId());

		return !(waitForQuorum(request) == null);
	}

	// TODO: if I remove a channel at line 121, it happens that I may request
	// the same index at line 144 and 188 causing an array out of bound
	// exception
	// I have no idea how to fix it
	private Message[] waitForQuorum(Message request) {

		int serverCount = n.getServerCount();
		ArrayList<Boolean> turns = n.getTurns();
		ArrayList<SocketChannel> chan = n.getServerChannels();
		Message[] values = new Message[serverCount];
		ArrayList<Status> status = new ArrayList<>(serverCount);
		ByteBuffer readBuffer = ByteBuffer.allocate(Node.BUFFER_SIZE);
		ByteBuffer writeBuffer = ByteBuffer.allocate(Node.BUFFER_SIZE);

		Message reply = new Message();

		// initialize status and service variables
		int ackCounter = 0;
		for (int i = 0; i < serverCount; i++) {
			values[i] = null;
			status.add(Status.NOTSENT);
		}

		writeBuffer.clear();
		writeBuffer.put(request.toString().getBytes());

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
					n.removeCrashedServer(i);
					status.remove(i);
					i--;
					serverCount = n.getServerCount();
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
						reply.fromString(message);
					} catch (IOException e) {
						System.out
								.println("Channel doesn't exist anymore, removing it");
						n.removeCrashedServer(i);
						status.remove(i);
						i--;
						serverCount = n.getServerCount();
						continue;
					}


					int sendID = reply.getSenderId();
					incrementFD(sendID);



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
							n.removeCrashedServer(i);
							status.remove(i);
							serverCount = n.getServerCount();
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

		n.setTurns(turns);
		return values;

	}

	private void incrementFD (int sendID){

		//resetting counter for the sender
		n.getCounter().set(sendID,0);

		for (int l = 1; l < n.getCounter().size(); l++) {



			// increment all the counters of the other servers if sender id is not the cycled id
			if (sendID == 0 || sendID == n.getId())
				break;
			if (sendID != l)

				n.getCounter().set(l, (n.getCounter().get(l)) + 1); // set
			// the cycled id as itself +1

		}

	}
}
