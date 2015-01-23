package subscriber;
import java.util.HashMap;
import java.io.*;
import org.apache.hadoop.io.WritableComparable;

/** 
 * The class is used in the first phase of the program. It contains an object of the 
 * subscriberInfo class and the a timestamp string. The subscriberInfo object contains
 * the subscriber information such as the service name, service type and the day of the 
 * access. The timestamp field holds the time of the access in terms of hours and 
 * minutes. The class implements the WritableComparable which contains methods required 
 * to allows the class objects to be used as keys in the map reduce operations. 
 * @author Sameer Jagdale
 */
public class SubscriberAccessInfo implements  WritableComparable<SubscriberAccessInfo>{
	private SubscriberInfo subInfo;
	private String timestamp;

/** 
 * Default constructor. Calls the default constructor of the subscriberInfo class 
 * and sets the timestamp to null. 
 */
	public SubscriberAccessInfo() {
		subInfo = new SubscriberInfo();	
		timestamp = null;
	}	

/** @constructor parameterized constructor. Accepts the subscriberInfo and the 
 * timestamp and assigns them to respective fields in the class. 
 * @param info object of type subscriberInfo which is assigned to the 
 * subscriberInfo object in the class. 
 * @param time assigned to the timestamp field. 
 */
	public SubscriberAccessInfo(SubscriberInfo info, String time) {
		subInfo = info;
		timestamp = time;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		subInfo.write(out);
		out.writeUTF(timestamp);		
  	}

	@Override
	public void readFields(DataInput in) throws IOException {
		subInfo.readFields(in);
		timestamp = in.readUTF();		
	}
	
	@Override
	public int compareTo(SubscriberAccessInfo other) {
		if(timestamp.compareTo(other.getTimestamp()) == 0) {
			return subInfo.compareTo(other.getSubInfo());
		}
		return timestamp.compareTo(other.getTimestamp());
	}
	
/** 
 * Setter method for the subscriberInfo object. 
 * @param info value which is assigned to the subscriberInfo field of the 
 * class.
 */ 
	public void setSubInfo(SubscriberInfo info) {
		subInfo = info;
	}
	

/** 
 * Getter method for the SubscriberInfo object in the class. 
 * @return Returns the subInfo object. 
 */
	public SubscriberInfo getSubInfo() {
		return subInfo;
	}
	
/** 
 * Setter method for the timestamp object. 
 * @param time value to which the timestamp object is 
 * to be set to. 
 */
	public void setTimestamp(String time) {
		timestamp = time;
	}

/**Getter method for the timestamp. 
 * @return returns the time stamp value 
 */
	public String getTimestamp() {
		return timestamp;
	}
	
	@Override 
	public String toString() {
		return subInfo.toString() + "\t" + timestamp;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj == null)  return false;
		if(!(obj instanceof SubscriberAccessInfo)) return false;
		SubscriberAccessInfo info = (SubscriberAccessInfo)obj;
		return subInfo.equals(info.getSubInfo()) &&
			timestamp.equals(info.getTimestamp());
		
	}

	@Override
	public int hashCode() {
		return subInfo.hashCode() + timestamp.hashCode();
	}

/** static method. Accepts a tab separated string and uses the fields of the string
 * to create a SubscriberAccessInfo object.
 * @param Tab separated string
 * @return an object of type SubscriberAccessInfo which is generated using the input 
 * string. 
 */
	public static SubscriberAccessInfo genSubscriberAccessInfo(String tsvLine) {
		String fields[] = tsvLine.split("[ \t]+");
		return new SubscriberAccessInfo(new SubscriberInfo(fields[0], fields[2], 
			Integer.parseInt(fields[1]), fields[3].substring(0,8)), fields[3].substring(8));
	}
	
	public static void main(String args[]) {
			if(args.length < 1) {
					System.out.println("No File name provided");
			}
			try {	
					BufferedReader br =  new BufferedReader(new FileReader((new File(args[0])).
											getAbsolutePath()));
					HashMap<SubscriberAccessInfo, Integer> map =  new HashMap<SubscriberAccessInfo, Integer>();
					for(int i = 0; i < 20; i++) {
							SubscriberAccessInfo info  = genSubscriberAccessInfo(br.readLine());
							// System.out.println(info.toString());
							if(map.containsKey(info)) {
									System.out.println("Match Found");
									map.put(info,Integer.valueOf(map.get(info).intValue() + 1));
							} else {
									map.put(info, new Integer(1));
							}
					}

					for( SubscriberAccessInfo info : map.keySet()) {
						System.out.println(info + " " +  map.get(info).toString());
					}
			} catch(IOException e) {
				e.printStackTrace();
			} 

	}

}

