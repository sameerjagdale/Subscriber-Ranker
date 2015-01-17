import java.util.Date;

public class SubscriberInfo {
	private String serviceType;
	private String serviceName;
	private long subscriberId;
	private Date timestamp;
	
	public SubscriberInfo(String serviceType, String serviceName,
			long subscriberId, Date timestamp) {
		this.serviceType = serviceType;
		this.serviceName = serviceName;
		this.subscriberId = subscriberId;
		this.timestamp = timestamp;
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

	public Date getTimestamp() {
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
	
	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}	
	
}
