import java.util.Date;

public class SubscriberInfo {
	private String serviceType;
	private String serviceName;
	private long subscriberId;
	private String timestamp;
	
	public SubscriberInfo(String serviceType, String serviceName,
			long subscriberId, String timestamp) {
		this.serviceType = serviceType;
		this.serviceName = serviceName;
		this.subscriberId = subscriberId;
		this.timestamp = timestamp;
	}
	
	public static SubscriberInfo  genSubscriberInfo(String tsvLine) {
		String[] fields = tsvLine.split(" ");
		if(fields.length < 4) {
			throw new IllegalArgumentException("Every line should have atleast four fields");
		}	
		return new SubscriberInfo(fields[0], fields[1], Integer.parseInt(fields[2]), 
			fields[3]);
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

	public String getTimestamp() {
		return timestamp;
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
	
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}	
	
}
