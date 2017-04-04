
public class Resource implements Comparable<Resource>{
	private String resource;
	private long totalSize; //total bytes of network communication over this resource
	
	public Resource(String resource, long size){
		this.resource = resource;
		this.totalSize = size;
	}
	
	public int hashCode(){
		return this.resource.hashCode(); 
	}
	
	public String toString(){
		return this.resource;
	}
	
	public int compareTo(Resource r){
		if( this.totalSize != r.totalSize )
			return Long.compare(this.totalSize, r.totalSize);  
		return r.resource.compareTo( this.resource ); 
	}
}
