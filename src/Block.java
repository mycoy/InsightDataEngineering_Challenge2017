
public class Block {
	private long failAttempt1;
	private long failAttempt2;
	private long failAttempt3; 
	
	public Block(){
		clearBlockFlags(); 
	}
	
	public void clearBlockFlags(){
		failAttempt1 = -1; 
		failAttempt2 = -1; 
		failAttempt3 = -1; 
	}
	
	//return 3: means in blocked status
	//return 0: means in complete clear status
	public int failureCount(){
		int count=0;
		if(failAttempt1!=-1)
			count++;
		if(failAttempt2!=-1)
			count++; 
		if(failAttempt3!=-1)
			count++;
		return count; 
	}

	public long getFailAttempt1() {
		return failAttempt1;
	}

	public void setFailAttempt1(long failAttempt1) {
		this.failAttempt1 = failAttempt1;
	}

	public long getFailAttempt2() {
		return failAttempt2;
	}

	public void setFailAttempt2(long failAttempt2) {
		this.failAttempt2 = failAttempt2;
	}

	public long getFailAttempt3() {
		return failAttempt3;
	}

	public void setFailAttempt3(long failAttempt3) {
		this.failAttempt3 = failAttempt3;
	}
}
