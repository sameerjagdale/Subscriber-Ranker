public class SubscriberAccessInfo {
	private SubscriberInfo subInfo;
	private String timestamp;
	
	public SubscriberAccessInfo(SubscriberInfo info, String time) {
		subInfo = info;
		timestamp = time;
	}
	
	public static SubscriberAccessInfo genSubscriberAccessInfo(String tsvLine) {
		String fields[] = tsvLine.split(" ");
		return new SubscriberAccessInfo(new SubscriberInfo(fields[0], fields[1], Integer.parseInt(fields[2])),
			 fields[3]);
		
	}
}

