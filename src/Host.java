
public class Host implements Comparable<Host>{
	private String host;
	private int totalAccessCount; //how many times this host accesses the site
	
	public Host(String host, int count){
		this.host = host;
		this.totalAccessCount=count;
	}
	
	public int hashCode(){
		return this.host.hashCode();
	}
	
	public String toString(){
		return host+","+totalAccessCount;
	}
	
	public int compareTo(Host h){
		if( this.totalAccessCount != h.totalAccessCount )  //firstly, order by the count, ascending
			return Integer.compare(this.totalAccessCount, h.totalAccessCount);
		return h.host.compareTo( this.host ); //secondly, order by host-name, descending
	}
}
