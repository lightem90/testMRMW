package EncoderDecoder;

import Structures.Counter;
import Structures.Message;
import Structures.Tag;
import Structures.View;

/**
 * Created by Matteo on 22/07/2015.
 */
public class EncDec {



    private final static String SEPARATOR = ",";
	private final static String TAG_SEPARATOR = ":";

	private final static int INVALID = -1;



	//TODO: checks
    public Message decode(String stringToDecode){

		Message ret = new Message();
		String reqT = String.valueOf(INVALID);
		View tmpV = new View(String.valueOf(INVALID));
		Tag tmpT = new Tag(INVALID,INVALID);
		int tmpS = INVALID;



		String[] tokens = stringToDecode.split(SEPARATOR);
		if (tokens.length == 4){

			reqT = tokens[0];
			tmpV.setValue(tokens[1]);
			tmpS = Integer.parseInt(tokens[3]);
			String tag_tokens[] = tokens[2].split(TAG_SEPARATOR);
			if (tag_tokens.length == 4){
				tmpT.setId(Integer.parseInt(tag_tokens[0]));
				tmpT.setLabel(Integer.parseInt(tag_tokens[1]));
				Counter tmp = new Counter(Integer.parseInt(tag_tokens[2]), Integer.parseInt(tag_tokens[3]));
				tmpT.getCounters().add(tmp);

			}


		}

		ret.setRequestType(reqT);
		ret.setSenderId(tmpS);
		ret.setView(tmpV);

        return ret;
    }



    public String encode (Message msgToEncode){


		String reqT = msgToEncode.getRequestType();
		View tmpV = msgToEncode.getView();
		Tag tmpT = msgToEncode.getTag();
		int tmpS = msgToEncode.getSenderId();

		StringBuilder sb = new StringBuilder();

		sb.append(reqT);
		sb.append(SEPARATOR);
		sb.append(tmpV.getValue());
		sb.append(SEPARATOR);
		sb.append(tmpT.getId() + TAG_SEPARATOR + tmpT.getLabel() + TAG_SEPARATOR + tmpT.getCounters().get(0).getId() + TAG_SEPARATOR + tmpT.getCounters().get(0).getCounter());
		sb.append(SEPARATOR);
		sb.append(tmpS);
		sb.append("&");



        return sb.toString();
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

	    //encodes counter to string using Node.SEPARATOR parameter
    public String counterToString(Counter c){

        return String.valueOf(id) + Node.SEPARATOR + String.valueOf(counter);
    }

    //decodes counter from string
    public void counterFromString(String s){

        String tokens[] = s.split(Node.SEPARATOR);

        if (tokens.length == 2) {
            this.id = Integer.parseInt(tokens[0]);
            this.counter = Long.parseLong(tokens[1]);
            }
        else System.out.print("Cannot get counter from string");
    }


    */


}