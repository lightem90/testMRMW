package com.robustMRMW;

import java.util.LinkedList;

public class Tag implements Comparable<Tag>{

	private int id;
	private int label;
	private LinkedList<Counter> counters;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getLabel() {
		return label;
	}

	public void setLabel(int label) {
		this.label = label;
	}

	public LinkedList<Counter> getCounters() {return counters;}

	public void setCounters(LinkedList<Counter> counters) {this.counters = counters;}


	//intializes a new Tag with an empty counter
	public Tag(int id, int label) {

		this.id = id;
		this.label = label;
		this.counters = new LinkedList<Counter>();


	}

	public Tag greatestTag(Tag t1, Tag t2) {

		if (t1.label > t2.label)
			return t1;

		//if labels are equal I check id (they cannot be equal)
		if (t1.label == t2.label) {
			if (t1.id > t2.id)
				return t1;
			else
				return t2;

		}
		
		//t2 is for sure > t1
		return t2;

	}

	//Convert tag to String using a new separator identify where the counter starts and then each element is separate with another one separator
	public String toString(){

		StringBuilder str = new StringBuilder(id + Node.SEPARATOR + label + Node.ARRAY_SEPARATOR);
		for (Counter c : counters)
			str.append(c.toString()+Node.COUNTER_SEPARATOR);

		//removes last character (a useless separator at the end)
		str.substring(0,str.length()-1);
		return toString();
	}

	//Decode tag from string using 3 different separators, no checks are performed atm
	public void fromString(String inputString){
		//separate id and label from counter
		String[] first_tokens = inputString.split(Node.ARRAY_SEPARATOR);
		String[] second_tokens = first_tokens[0].split(Node.SEPARATOR);
		this.id = Integer.parseInt(second_tokens[0]);
		this.label = Integer.parseInt(second_tokens[1]);
		this.counters = new LinkedList<Counter>();
		String[] third_tokens = first_tokens[1].split(Node.COUNTER_SEPARATOR);
		for (int i = 0; i<third_tokens.length-1; i+=2){

			Counter tmp = new Counter(Integer.parseInt(third_tokens[i]), Integer.parseInt(third_tokens[i+1]));
			counters.add(tmp);


		}



	}

	@Override
	public int compareTo(Tag anotherTag) {
		if(this.label == anotherTag.getLabel())
			return Integer.compare(this.id,anotherTag.getId());
		return Integer.compare(this.label,anotherTag.getLabel());
	}
	@Override
	public int hashCode() {
		return 0;
	}
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		return (this.compareTo((Tag)o)==0);
	}
}
