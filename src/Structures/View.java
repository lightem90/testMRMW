package Structures;

import com.robustMRMW.Node;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;



/* Used to store the information about the nodes this node considers online, the information is encode both in a string (by separating ids) and in an arraylist to retrieve them faster */
public class View {

	private static final int PORT_THRESHOLD = 1000; // limit to recognize a port number
	private static final String ID_SEPARATOR = "/";

	public enum Label {
		FIN, PRE
	}

	private String value;
	private Label label;
	private Node.Status status;
	//this is te content of the value string (a list of id's) in this way we can compare it faster maybe instead of just the string
	//NB: LAST POSITION OF THE ARRAY IS NOT AN ID IS THE STATUS OF THE VIEW
	private ArrayList<Integer> idArray;

	/* Constructor sets by default FIN and NONE appending the status at the end of the value string */
	public View(String value){

		this.label = Label.FIN;
		this.status = Node.Status.NONE;

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
		idArray.add(Node.Status.NONE.GetValue());

		label = Label.FIN;
		status = Node.Status.NONE;
		sb.append(Node.Status.NONE.GetValue());
		value = sb.toString();

	}

	/* Next two methods are used to set the string or the array when we acquire the view by array or string, in this way in the future we can refer to a view with both data structures */
	public void setArrayFromValueString(){

		if (!(value == null) && !(value.isEmpty())) {

			String[] tokens = value.split(ID_SEPARATOR);
			idArray = new ArrayList<>();

			int statusFromString = -1;

			for (int i = 0; i < tokens.length - 1; i++) {
				int element = Integer.parseInt(tokens[i]);
				if (element > PORT_THRESHOLD)
					idArray.add(element);
				else
					statusFromString = element;
			}

			//Sets the status according to the last int element of the array (should be 0-1-2-3), if it's not an identifier it's another node id
			if (statusFromString != -1) {
				idArray.add(statusFromString);
				status = Node.Status.valueOf(String.valueOf(statusFromString));
			} else {
				status = Node.Status.NONE;
				idArray.add(status.GetValue());
			}
		}

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

	public Node.Status getStatus() {
		return status;
	}

	public void setStatus(Node.Status status) {

		if (status == null)
			return;
		//replace the old array with an updated one with the correct status
		for (int i = 0; i<idArray.size();i++)
		{
			if (idArray.get(i) < PORT_THRESHOLD)
				idArray.set(i,status.GetValue());
		}
		//Replace the old string with the new string with same id but new status
		StringBuilder sb = new StringBuilder();
		String[] tokens = value.split(ID_SEPARATOR);

		boolean statusWasPresent = false;
		for (int i = 0; i<tokens.length;i++)
		{
			if (Integer.parseInt(tokens[i]) < PORT_THRESHOLD) {
				sb.append(status.GetValue());
				statusWasPresent = true;
			}
			else
				sb.append(tokens[i]);
			sb.append(ID_SEPARATOR);
		}
		if (!statusWasPresent)
			sb.append(Node.Status.NONE.GetValue());

		value = sb.toString();
		this.status = status;
	}
}
