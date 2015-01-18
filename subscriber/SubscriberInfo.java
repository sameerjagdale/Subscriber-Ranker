package subscriber;

import java.io.BufferedReader;
import java.util.HashMap;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;

public class SubscriberInfo {
	private String serviceType;
	private String serviceName;
	private long subscriberId;
	private String day;
	
	public SubscriberInfo(String serviceType, String serviceName,
			long subscriberId, String day) {
		this.serviceType = serviceType;
		this.serviceName = serviceName;
		this.subscriberId = subscriberId;
		this.day = day;
	}
	
	public static SubscriberInfo  genSubscriberInfo(String tsvLine) {
		String[] fields = tsvLine.split("[\t ]+");
		if(fields.length < 3) {
			throw new IllegalArgumentException("Every line should have atleast three fields");
		}	
		return new SubscriberInfo(fields[0], fields[2], Integer.parseInt(fields[1]),fields[3].substring(0,8));
	}

	public String getServiceType() {
		return serviceType;
	}
	
	public String getServiceName() {
		return serviceName;
	}
	
	public long getSubscriberId() {
		return subscriberId;
	}

	public void setServiceType(String serviceType) {
		this.serviceType = serviceType;
	}
		
	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}
	
	public void setSubscriberId(long subscriberId) {
		this.subscriberId = subscriberId;
	}
	
	@Override
	public String toString() {
		return serviceType + "\t" + serviceName + "\t" + subscriberId + "\t" + day;
	}	
	
	public String getDay() {
		return day;
	}
	public void setDay(String day) {
		this.day = day;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj == null) return false;
		if(!(obj instanceof SubscriberInfo)) {
			return false;
		}
		SubscriberInfo info = (SubscriberInfo)obj;
		return  serviceName.equals(info.getServiceName()) &&
				serviceType.equals(info.getServiceType()) &&
				(subscriberId == info.getSubscriberId()) &&
				day.equals(info.getDay());
	}

	@Override
	public int hashCode() {
		return serviceName.hashCode() +
			serviceType.hashCode() +
			Long.valueOf(subscriberId).hashCode() + 
			day.hashCode();
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
