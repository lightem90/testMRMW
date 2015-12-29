package Structures;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;


/* Used to store the information about the nodes this node considers online, the information is encode both in a string (by separating ids) and in an arraylist to retrieve them faster */
public class View {

	private static final String ID_SEPARATOR = "/";

	public enum Label {
		FIN, PRE
	}
	public enum Status {
		NONE(0),
		MULTICAST(1),
		PROPOSE(2),
		INSTALL(3);
		private final int value;

		private Status(int value)
		{this.value = value;}

		public int GetValue()
		{return this.value;}
	}

	private String value;
	private Label label;
	private Status status;
	//this is te content of the value string (a list of id's) in this way we can compare it faster maybe instead of just the string
	//NB: LAST POSITION OF THE ARRAY IS NOT AN ID IS THE STATUS OF THE VIEW
	private ArrayList<Integer> idArray;

	/* Constructor sets by default FIN and NONE */
	public View(String value){

		this.label = Label.FIN;
		this.status = Status.NONE;

		StringBuilder sb = new StringBuilder(value);
		sb.append(ID_SEPARATOR);
		sb.append(this.status.GetValue());
		this.value = sb.toString();
	}


	//AD-HOC constructor to update view from Failure Detector, it fills the arrayList, the string to be send and the label variable
	//We need this to update the local node view every time a node comes online/offline
	public View (Set s){

		idArray = new ArrayList<>(s.size());
		StringBuilder sb = new StringBuilder();


		Iterator<Integer> it = s.iterator();
		while(it.hasNext()){
			Integer curr = it.next();

			idArray.add(curr);
			sb.append(curr);
			sb.append(ID_SEPARATOR);

		}

		//Last position of the array is the view status
		idArray.add(Status.NONE.GetValue());

		label = Label.FIN;
		status = Status.NONE;
		value = sb.toString();

	}

	/* Next two methods are used to set the string or the array when we acquire the view by array or string, in this way in the future we can refer to a view with both data structures */
	public void setArrayFromValueString(){

		String[] tokens = value.split(ID_SEPARATOR);
		idArray = new ArrayList<>();

		int i;
		for (i = 0;i <tokens.length-1; i++)
			idArray.add(Integer.parseInt(tokens[i]));

		//Sets the status according to the last int element of the array (should be 0-1-2-3)
		status = Status.values()[(Integer.parseInt(tokens[i]))];

	}

	public void setStringFromArrayString(){

		StringBuilder sb = new StringBuilder();

		if (!(idArray == null) && !(idArray.isEmpty())){

			for (Integer i : idArray){

				sb.append(i);
				sb.append(ID_SEPARATOR);

			}
			sb.append(status.GetValue());
			sb.append(ID_SEPARATOR);
			value = sb.toString();


		}

	}


	/* Getters and Setters */
	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public Label getLabel() {
		return label;
	}

	public void setLabel(Label label) {
		this.label = label;
	}

	public ArrayList<Integer> getIdArray() {
		return idArray;
	}

	public void setIdArray(ArrayList<Integer> idArray) {
		this.idArray = idArray;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}
}
