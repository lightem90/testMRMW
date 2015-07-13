package com.robustMRMW;

import java.util.ArrayList;

public class View {

	public enum Status {
		FIN, PRE;

	}
	private String value;
	private Status status ;

	//this is te content of the value string (a list of id's) in this way we can compare it faster maybe instead of just the string
	private ArrayList<Integer> idArray;


	public View(String value){
		this.value = value;
		this.status = Status.FIN;
	}


	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}


	//building string value with all id
	public void arrayToString(){
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i<idArray.size(); i++){

			sb.append(idArray.get(i));
			sb.append(Node.SEPARATOR);

		}
		sb.substring(0,sb.length()-1);
		value = sb.toString();

	}

	//filling the array with ids from the value string
	public void arrayFromString(){

		String[] tokens = value.split(Node.SEPARATOR);
		for (int i=0;i<tokens.length; i++){

			idArray.set(i,Integer.parseInt(tokens[i]));


		}


	}


	public ArrayList<Integer> getIdArray() {
		return idArray;
	}

	public void setIdArray(ArrayList<Integer> idArray) {
		this.idArray = idArray;
	}
}
