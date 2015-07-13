package com.robustMRMW;

public class Message {

	private String requestType; // type of request
	private View view;
	private Tag tag;
	private int senderId;

	/* Creates a not linked message */
	public Message(String req, Tag tag, View view, int id) {
		this.setRequestType(req);
		this.view = view;
		this.tag = tag;
		this.senderId = id;
	}

	public Message() {
	}

	public View getView() {
		return view;
	}

	public void setView(View systemView) {
		this.view = systemView;
	}

	public String getRequestType() {
		return requestType;
	}

	public void setRequestType(String requestType) {
		this.requestType = requestType;
	}

	public String toString() {
		return requestType + "," + tag.toString() + "," + view.toString()+ ","
				+ String.valueOf(senderId) + "&";
	}

	public Message fromString(String inputString) {

		String[] tokens = inputString.split(",");
		requestType = tokens[0];
		tag = new Tag(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]));
		view = new View(tokens[3]);
		tokens[4] = tokens[4].replace("&","");
		senderId = Integer.parseInt(tokens[4]);
		return this;
	}

	public Tag getTag() {
		return tag;
	}

	public void setTag(Tag tag) {
		this.tag = tag;
	}

	public int getSenderId() {
		return senderId;
	}

	public void setSenderId(int senderId) {
		this.senderId = senderId;
	}
}
