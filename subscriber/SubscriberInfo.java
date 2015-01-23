package subscriber;
import java.io.BufferedReader;
import java.util.HashMap;
import org.apache.hadoop.io.WritableComparable;
import java.io.*;

/** 
 * The class acts as a container for the first phase of the program. 
 * Contains information which is read from the input files. 
 * Since the class acts as a key in the map reduce operation, the class implements 
 * the WritableComparable interface and overrides the required methods. 
 * @author Sameer Jagdale
 */
public class SubscriberInfo implements WritableComparable<SubscriberInfo> {
/** 
 * contains the value of the service type field of the record. 
 */
	private String serviceType;
/** 
 * contains the value of the service name field of the record. 
 */
	private String serviceName;
/** 
 * contains the value of the subscriber ID field of the record. 
 */
	private long subscriberId;
/** 
 * contains the value of the day field of the record. The day value is separated from the timestamp
 * and stored. The rest of the timestamp is stored separately. 
 */
	private String day;
/** 
 * Default constructor. Sets the serviceName, serviceType and day values to null. 
 * Subscriber ID is set to 0.
 */	
	public SubscriberInfo() {
		serviceName = null;
		serviceType = null;
		subscriberId = 0;
		day = null;
	}	
/** 
 * Parameterized constructor. The constructor accepts four arguments, one each for the four member 
 * variables. 
 * @param serviceType value is assigned to the serviceType variable of the object. 
 * @param serviceName value is assigned to the serviceName variable of the object.
 * @param subscriberId value is assigned to the subscriberId variable of the object.
 * @param day value is assigned to the day variable of the object.
 */
	public SubscriberInfo(String serviceType, String serviceName,
			long subscriberId, String day) {
		this.serviceType = serviceType;
		this.serviceName = serviceName;
		this.subscriberId = subscriberId;
		this.day = day;
	}
/** 
 * Method is overriden from the WritableComparable interface. 
 * Defines how the data should be written to the output stream. 
 * @param out defines the output stream to which the data is to be written. 
 */	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(serviceType);
		out.writeLong(subscriberId);
		out.writeUTF(serviceName);
		out.writeUTF(day);
  	}

/** 
 * Method is overriden from the WritableComparable interface. 
 * Defines how the data should be read from the input stream. 
 * @param in defines the input stream from which the data is to be read. 
 */
	@Override	
	public void readFields(DataInput in) throws IOException {
		serviceType = in.readUTF();	
		subscriberId = in.readLong();
		serviceName = in.readUTF();	
		day = in.readUTF();	
	}

/** 
 * Method is overriden from the WritableComparable interface. 
 * Defines ordering of SubscriberInfo objects. 
 * The objects are ordered first by the day, then by the subsriber ID, by the serviceName
 * and finally by the serviceType. 
 * @param other the other object to which this object is to be compared. 
 * @return method returns a negative value of the other object is greater than this object, 
 * a positive value of this object is greaten than the other object and 0 if they are equal. 
 */
	@Override
	public int compareTo(SubscriberInfo other) {
		if(day.compareTo(other.getDay()) == 0)  {
			if((new Long(subscriberId)).compareTo(new Long(other.getSubscriberId())) == 0) {
				if(serviceName.compareTo(other.getServiceName()) == 0 ) {
					return serviceType.compareTo(other.getServiceType());
				}	
				return serviceName.compareTo(other.getServiceName());
			}
			return (new Long(subscriberId)).compareTo(new Long(other.getSubscriberId()));
		}
		return day.compareTo(other.getDay());
	}

/** 
 * Static method. Generates a SubscriberInfo object from a string. 
 * The string format is predefined. The fields in the strings have to be tab 
 * separated. The fields should be in the following format  :
 * service_name service_type subscriberId timestamp. 
 * @param tsvLine a string containing the tab separated fields. 
 * @return a SubscriberInfo object constructed using the fields from the 
 * input string. 
 */
	public static SubscriberInfo  genSubscriberInfo(String tsvLine) {
		String[] fields = tsvLine.split("[\t ]+");
		if(fields.length < 3) {
			throw new IllegalArgumentException("Every line should have atleast three fields");
		}	
		return new SubscriberInfo(fields[0], fields[2], Integer.parseInt(fields[1]),fields[3].substring(0,8));
	}

/** 
 * Getter function for the serviceType. 
 * @return returns the service type value. 
 */
	public String getServiceType() {
		return serviceType;
	}
	
/** 
 * Getter function for the serviceName. 
 * @return returns the value of the service name. 
 */
	public String getServiceName() {
		return serviceName;
	}
	
/** 
 * Getter function for the subscriberId. 
 * @return the value of the subscriberId. 
 */
	public long getSubscriberId() {
		return subscriberId;
	}
/** Getter function for the day field. 
 * @return returns the value of the day string. 
 */
	public String getDay() {
		return day;
	}

/** 
 * Setter function for the service type field of the class.
 * @param serviceType string object which is asssigned to the serviceType 
 * member variable of the class. 
 */
	public void setServiceType(String serviceType) {
		this.serviceType = serviceType;
	}
		
/** 
 * Setter function for the service name field of the class. 
 * @param serviceName string object which is assigned to the 
 * service name field of the class. 
 */
	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

/**
 * Setter function for the subscriber id field. 
 * @param subscriberId assgined to the subscriberId field of the
 * class. 	
 */
	public void setSubscriberId(long subscriberId) {
		this.subscriberId = subscriberId;
	}

/** 
 * Setter function for the day field.
 * @param day string object which is assigned to the day field of the 
 * class.
 */
	public void setDay(String day) {
		this.day = day;
	}

	@Override
	public String toString() {
		return serviceType + "\t" + subscriberId + "\t" +serviceName + "\t" +   day;
	}	
	

	@Override
	public boolean equals(Object obj) {
		if(obj == null) return false;
		if(!(obj instanceof SubscriberInfo)) {
			return false;
		}
		SubscriberInfo info = (SubscriberInfo)obj;
		return toString().equals(info.toString());
	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	public static void main(String args[]) {
			if(args.length < 1) {
					System.out.println("No File name provided");
			}
			try {	
					BufferedReader br =  new BufferedReader(new FileReader((new File(args[0])).
						getAbsolutePath()));
					HashMap<SubscriberInfo, Integer> map =  new HashMap<SubscriberInfo, Integer>();
					for(int i = 0; i < 20; i++) {
							SubscriberInfo info  = genSubscriberInfo(br.readLine());
							// System.out.println(info.toString());
							if(map.containsKey(info)) {
								System.out.println("Match Found");
								map.put(info,Integer.valueOf(map.get(info).intValue() + 1));
							} else {
								map.put(info, new Integer(1));
							}
					}
					
					for( SubscriberInfo info : map.keySet()) {
						System.out.println(info + " " +  map.get(info).toString());
					}
			} catch(IOException e) {
				e.printStackTrace();
			} 

	}

}
