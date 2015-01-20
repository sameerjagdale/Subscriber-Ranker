package subscriber;

import java.util.HashMap;
import java.io.*;
import org.apache.hadoop.io.WritableComparable;

public class SubscriberAccessInfo implements  WritableComparable<SubscriberAccessInfo>{
	private SubscriberInfo subInfo;
	private String timestamp;
	
	public void write(DataOutput out) throws IOException {
		subInfo.write(out);
		out.writeUTF(timestamp);		
  	}
	public SubscriberAccessInfo() {
		subInfo = new SubscriberInfo();	
		timestamp = null;
	}	
	public void readFields(DataInput in) throws IOException {
		subInfo.readFields(in);
		timestamp = in.readUTF();		
	}
	
	@Override
	public int compareTo(SubscriberAccessInfo other) {
		return timestamp.compareTo(other.getTimestamp());		
	}

	public SubscriberAccessInfo(SubscriberInfo info, String time) {
		subInfo = info;
		timestamp = time;
	}
	
	public void setSubInfo(SubscriberInfo info) {
		subInfo = info;
	}
	
	public SubscriberInfo getSubInfo() {
		return subInfo;
	}
	
	public void setTimestamp(String time) {
		timestamp = time;
	}
	
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

