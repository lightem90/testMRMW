package EncoderDecoder;

import Structures.Message;

/**
 * Created by Matteo on 22/07/2015.
 */
public class EncDec {



    public static String SEPARATOR = ",";
    public static String ARRAY_SEPARATOR = "?";
    public static String COUNTER_SEPARATOR = ":";



    public Message decode(String stringToDecode){




        return new Message();
    }


    public String enccode (Message msgToEncode){





        return new String ();
    }


    /*
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


    */


}
