package Structures;

public class Message {

	private String requestType; // type of request
	private View view;
	private Tag tag;
	private int senderId;

	public Message() {}

	/* Creates a not linked message */
	public Message(String req, Tag nTag, View nView, int sId) {
		requestType=req;
		view = nView;
		tag = nTag;
		senderId = sId;
	}



	/* Getters and Setters */
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
