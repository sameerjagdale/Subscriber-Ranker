package subscriber;
import java.util.HashMap;
import java.io.*;
import org.apache.hadoop.io.WritableComparable;

public class DayLogger implements WritableComparable<DayLogger> {
	private String serviceName;
	private String serviceType;
	private long subscriberId;		
	private int count;
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(serviceName);
		out.writeUTF(serviceType);
		out.writeLong(subscriberId);
		out.writeInt(count);
	}

	@Override	
	public void readFields(DataInput in) throws IOException {
		serviceName = in.readUTF();
		serviceType = in.readUTF();
		subscriberId = in.readLong();
		count = in.readInt();
	}
	
	@Override
	public int compareTo(DayLogger other) {
		return count - other.getCount();
	}
	
	@Override 	
	public String toString() {
		return serviceName + "\t" + subscriberId + "\t" +  serviceType + "\t" + count;
	}
	
	public DayLogger() {
		count = 0;
		serviceName = null;
		serviceType = null;
		subscriberId = 0;
	}
	
	public DayLogger(String serviceName, long subscriberId, String serviceType,  int count) {
		this.serviceName = serviceName;
		this.serviceType = serviceType;
		this.subscriberId = subscriberId;
		this.count = count;
	}
	
	public int getCount() {
		return count;
	}

	public void setCount() {
		this.count = count;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj == null) {
			return false;
		}
		if(!(obj instanceof DayLogger)) {
			return false;
		}
		DayLogger other = (DayLogger) obj;
		return count == other.getCount() &&
			subscriberId == other.getSubscriberId() &&
			serviceName.equals(other.getServiceName()) &&
			serviceType.equals(other.getServiceType());
	}
	
	@Override 
	public int hashCode() {
		return (new Integer(count)).hashCode() + serviceName.hashCode() 
			+ serviceType.hashCode() + (new Long(subscriberId)).hashCode();
	}
	
	public String getServiceType() {
		return serviceType;
	}
	
	public void setServiceType(String serviceType) {
		this.serviceType = serviceType;
	}
	
	public String getServiceName() {
		return serviceName;
	}
	
	public void setServiceName() {
		this.serviceName = serviceName;
	}
	
	public long getSubscriberId() {
		return subscriberId;
	}
	
	public void setSubscriberId(long sub) {
		subscriberId = sub;
	}
}
