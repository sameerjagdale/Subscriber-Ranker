package subscriber;
import java.util.HashMap;
import java.io.*;
import org.apache.hadoop.io.WritableComparable;

public class DayLogger implements WritableComparable<DayLogger> {
	private String date;
	private int count;
	public void write(DataOutput out) throws IOException {
		out.writeUTF(date);
		out.writeInt(count);
	}
	
	public void readFields(DataInput in) throws IOException {
		date = in.readUTF();
		count = in.readInt();
	}
	
	public int compareTo(DayLogger other) {
		if(date.compareTo(other.getDate()) == 0) {
			return (new Integer(count)).compareTo(new Integer(other.getCount()));
		}	
		return date.compareTo(other.getDate());
	}
	
	public String toString() {
		return date + "\t" + count;
	}
	
	public DayLogger() {
		date = null;
		count = 0;
	}
	
	public DayLogger(String date, int count) {
		this.date = date;
		this.count = count;
	}
	
	public String getDate() {
		return date;
	}
	
	public void setDate() {
		this.date = date;
	}
	
	public int getCount() {
		return count;
	}

	public void setCount() {
		this.count = count;
	}
}
