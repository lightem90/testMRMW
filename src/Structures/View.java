package Structures;

import com.robustMRMW.Node;

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

	public ArrayList<Integer> getIdArray() {
		return idArray;
	}

	public void setIdArray(ArrayList<Integer> idArray) {
		this.idArray = idArray;
	}
}
