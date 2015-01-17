public class SubscriberInfo {
	private String serviceType;
	private String serviceName;
	private long subscriberId;
	
	public SubscriberInfo(String serviceType, String serviceName,
			long subscriberId) {
		this.serviceType = serviceType;
		this.serviceName = serviceName;
		this.subscriberId = subscriberId;
	}
	
	public static SubscriberInfo  genSubscriberInfo(String tsvLine) {
		String[] fields = tsvLine.split(" ");
		if(fields.length < 3) {
			throw new IllegalArgumentException("Every line should have atleast three fields");
		}	
		return new SubscriberInfo(fields[0], fields[1], Integer.parseInt(fields[2]));
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
	
}
