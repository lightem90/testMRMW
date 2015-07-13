package com.robustMRMW;

import java.net.SocketAddress;

public class NodeInfo {

	//preso dall'algoritmo, non so a cosa serva
	public enum Status {
		MULTICAST, UNICAST;

	}

	int id;
	SocketAddress address;
	View proposedView;
	Status status;
	boolean isMaster;
	boolean isAlive;
	int counter;
	//Tag localTag;  ?????

	
	public NodeInfo(){}

	public NodeInfo(int newId, SocketAddress newAddress) {

		id = newId;
		address = newAddress;

	}



	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public SocketAddress getAddress() {
		return address;
	}

	public void setAddress(SocketAddress address) {
		this.address = address;
	}

	public View getProposedView() {
		return proposedView;
	}

	public void setProposedView(View proposedView) {
		this.proposedView = proposedView;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public boolean checkMaster() {
		return isMaster;
	}

	public void setIsMaster(boolean isMaster) {
		this.isMaster = isMaster;
	}
	public boolean checkAlive() {
		return isAlive;
	}

	public void setIsAlive(boolean isAlive) {
		this.isAlive = isAlive;
	}

	

}
