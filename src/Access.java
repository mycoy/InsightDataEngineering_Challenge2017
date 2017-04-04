
public class Access implements Comparable<Access>{
	private String timestamp; 
	private int count; //total access count in the timestamp
	
	public Access(String timestamp, int count){
		this.timestamp = timestamp;
		this.count = count;
	}
	
	public void increaseCountByOne(){
		this.count++; 
	}
	
	public int getCount(){
		return this.count; 
	}
	
	public void setCount(int count){
		this.count = count;
	}
	
	public String getTimestamp(){
		return this.timestamp; 
	}
	
	public int hashCode(){
		return this.timestamp.hashCode();
	}
	
	public int compareTo(Access a){
		if( this.count != a.count )
			return Integer.compare(this.count, a.count);  
		return a.timestamp.compareTo( this.timestamp ); 
	}
	
	public String toString(){
		return timestamp+","+count;
	}
}
