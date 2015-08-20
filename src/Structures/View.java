package Structures;

import com.robustMRMW.Node;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

public class View {

	private static final String ID_SEPARATOR = "/";

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


	//AD-HOC constructor to update view from Failure Detector, it fills the arrayList, the string to be send and the status variable
	public View (Set s){

		idArray = new ArrayList<Integer>(s.size());
		StringBuilder sb = new StringBuilder();


		Iterator<Integer> it = s.iterator();
		while(it.hasNext()){
			Integer curr = it.next();

			idArray.add(curr);
			sb.append(curr);
			sb.append(ID_SEPARATOR);

		}

		status = Status.FIN;
		value = sb.toString();

	}

	public void setArrayFromValueString(){

		String[] tokens = value.split(ID_SEPARATOR);
		idArray = new ArrayList<Integer>();

		for (int i = 0;i <tokens.length; i++)
		idArray.add(Integer.parseInt(tokens[i]));

	}

	public void setStringFromArrayString(){

		StringBuilder sb = new StringBuilder();

		if (!(idArray == null) && !(idArray.isEmpty())){

			for (Integer i : idArray){

				sb.append(i);
				sb.append(ID_SEPARATOR);

			}
			value = sb.toString();


		}

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
