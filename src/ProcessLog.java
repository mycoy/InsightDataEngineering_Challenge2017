/**
 * Author: Wenzhao Zhang, wzhang27@ncsu.edu
 */
import java.io.*;
import java.util.*;
import java.text.*;

public class ProcessLog {
	private static final int RESULT_SIZE=10; //result size for F1, F2, F3
	private static final int INPUT_SECTIONS=10; //input sections (seperated by ' ') in a line 
	private static final int SIXTY_MIN_MILLI = 3600000; //60 minutes in milli-second   
	private static final int TWENTY_SEC_MILLI = 20000; //20 seconds in milli-second   
	private static final int FIVE_MIN_MILLI = 300000; //5 minutes in milli-second 
	private static final String LOGIN = "/login";
	private static final int LOGIN_FAIL = 401;
	
	//hard-coded paths only for internal testing
	private static String inputTest = "/media/data2/zwz/working/eclipse-java-workspace/fansite-analytics-challenge/insight_testsuite/tests/test_features/log_input/log.txt";
	//private static String inputTest = "/media/data2/zwz/working/eclipse-java-workspace/fansite-analytics-challenge/log_input/log.txt";
	
	private static String input=null;
	private static String outputF1=null;
	private static String outputF2=null;
	private static String outputF3=null;
	private static String outputF4=null;

	//To support feature 1
	private Map<String, Integer> hostAccessInfo = new HashMap<String, Integer>(); 
	
	//To support feature 2
	private Map<String, Long> resourceAccessedInfo = new HashMap<String, Long>(); 
	
	//To support feature 3
	private int accessedCount=0; //total accessed count within a time window (60 minutes)
	private Map<Long, Access> accessInfo = new LinkedHashMap<Long, Access>();
	private Calendar firstAccessCalendar = null; 
	private PriorityQueue<Access> accessWindowsPQ = new PriorityQueue<Access>(); 
	
	//To support feature 4
	private Map<String, Block> blockInfo = new HashMap<String, Block>(); 
	private List<String> blockedLog = new ArrayList<String>();
	
	
	private void process( String inputFile ){
		DateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
		Calendar cld = Calendar.getInstance();
		
		try {
            BufferedReader br = new BufferedReader(new FileReader( inputFile ) );
            String line;
            while ((line = br.readLine()) != null) {
            	String[] temp = line.split(" ");
            	if(temp.length < INPUT_SECTIONS) //invalid format
            		continue;
            	
            	//#update info for feature 1
            	String host = temp[0];
            	if( !hostAccessInfo.containsKey(host) )
            		hostAccessInfo.put(host, 0);
            	hostAccessInfo.put(host, hostAccessInfo.get(host)+1);
            	
            	String timestamp = temp[3].replace("[", "") + " " + temp[4].replace("]", "");
            	String resource = temp[6];
            	int code = 0;
            	int size = 0;
            	
            	try{
            		code = Integer.parseInt(temp[8]);
            	}catch( NumberFormatException e ){
            	}
            	
            	//#update info for feature 2
            	try{
            		size = Integer.parseInt( temp[9] );
            		if( !resourceAccessedInfo.containsKey(resource) )
            			resourceAccessedInfo.put(resource, 0l);
            		resourceAccessedInfo.put(resource, resourceAccessedInfo.get(resource)+size); 
            	}catch( NumberFormatException e ){
            	}
            	
            	//#update info for feature 3 
            	cld.setTime( df.parse(timestamp) ); //update timestamp info for current line; this parsing is very time-costly 
            	long milli = cld.getTimeInMillis(); //the timestamp of current record
            	//long milli = df.parse(timestamp).getTime();  //doesn't save much time
            	if( firstAccessCalendar == null ){  //init status 
            		firstAccessCalendar = Calendar.getInstance();
            		firstAccessCalendar.setTime( df.parse(timestamp) );
            	}
            		
            	if( accessInfo.containsKey(milli) ){ //update an existing access record 
            		accessedCount++;
            		accessInfo.get(milli).increaseCountByOne();
            	}else{ //if having an access with a new timestamp
            		Access ac = new Access(timestamp, 1); 
            		
            		//while, not if
            		while( milli - firstAccessCalendar.getTimeInMillis() >= SIXTY_MIN_MILLI ){ 
            			//if the new timestamp is out of the 60-min-window, which starts with "firstAccessCalendar",  
            			//	then we have a completed 60-min-window  
            			Access firstAccess = accessInfo.remove( firstAccessCalendar.getTimeInMillis() ); //remove first
            			int bakCount = firstAccess.getCount();
            			firstAccess.setCount( accessedCount ); //set total count of the 60-min window
            			accessWindowsPQ.add( firstAccess );
            			if( accessWindowsPQ.size() > RESULT_SIZE )
            				accessWindowsPQ.poll();
            			accessedCount -= bakCount; //deduct the access-count of the first timestamp
            			
            			//"A 60-minute window can be any 60 minute long time period, windows don't have to start at a time when an event occurs."
            			//		so, it's necessary to try every second
            			firstAccessCalendar.add(Calendar.SECOND, 1);
            			if( !accessInfo.containsKey( firstAccessCalendar.getTimeInMillis() ) ) //if map doesn't contain this new window-starting-boundary
            				accessInfo.put( firstAccessCalendar.getTimeInMillis(), new Access( df.format( firstAccessCalendar.getTime() ), 0  ) );
            		}
            		
            		accessInfo.put( milli, ac );
            		accessedCount++;
            	} 
            	
            	//#update info for feature 4
            	if( resource.equals(LOGIN) ){ //detect a login
            		if( code==LOGIN_FAIL ){ //login fail
                		if( blockInfo.containsKey(host) ) { //need to check previous failure records for this host
                			Block bck = blockInfo.get(host);  
                			
                			switch( bck.failureCount() ){
                			case 0:  //previous in complete cleared status
                				bck.setFailAttempt1( milli );
                				break;
                			case 1:  //previous, there is one failure record
                				if( milli - bck.getFailAttempt1()<=TWENTY_SEC_MILLI ) //still in 20sec window
                					bck.setFailAttempt2( milli );
                				else{ //out of the 20sec window
                					bck.clearBlockFlags();
                					bck.setFailAttempt1( milli );
                				}
                				break;
                			case 2:  //previous, there are two failure records
                				if( milli - bck.getFailAttempt1()<=TWENTY_SEC_MILLI ) //still in 20sec window
                					bck.setFailAttempt3( milli );
                				else{ //out of the 20sec window
                					bck.clearBlockFlags();
                					bck.setFailAttempt1( milli );
                				}
                				break;
                			
                			case 3:  //in blocked status
                				if( milli - bck.getFailAttempt3()<=FIVE_MIN_MILLI ) //within the 5min logging-window
                					blockedLog.add(line); //within 5-minutes, add to output log buffer
                				else{ //outside of the the 5min logging-window, reset the 20sec consecutive window 
                					bck.clearBlockFlags();
                					bck.setFailAttempt1( milli );
                				}
                				break;
                			}
                		}else{ //no previous failure records for this host
                			Block bck = new Block();
                			bck.setFailAttempt1( milli );
                			blockInfo.put(host, bck); 
                		}
            		}else if( code!=LOGIN_FAIL && blockInfo.containsKey(host) ){ //login succeed, current host in map
            			Block bck = blockInfo.get(host);
            			
            			switch( bck.failureCount() ){
            			case 0:  //a success login (no matter within 5min or not), will clear previous one/two failure records
            			case 1:
            			case 2:
            				bck.clearBlockFlags();
            				break;
            			case 3:  //the host is blocked
            				if( milli-bck.getFailAttempt3()>FIVE_MIN_MILLI )  //a five-min later success login can clear a previous blocked status
            					bck.clearBlockFlags();
            				else if( milli-bck.getFailAttempt3()<=FIVE_MIN_MILLI ) //still within 5min-window, add to output log buffer 
            					blockedLog.add(line); 
            				break;
            			}
            		}

            	}
            	
            	//System.out.println( host + "  " + timestamp + "  " + resource + "  " + code + "  " + size );
            }
        }catch( IOException e){
        	System.out.println(e);
        }catch( ParseException e ){
        	System.out.println(e);
        } 
	}
	
	private void outputF1(){
		PriorityQueue<Host> pq = new PriorityQueue<Host>();
		for(Map.Entry<String, Integer> e : hostAccessInfo.entrySet()){
			pq.add( new Host( e.getKey(), e.getValue() ) );
			
			if(pq.size() > RESULT_SIZE)
				pq.poll();
		}
		List<Host> buffer = new ArrayList<Host>(RESULT_SIZE);
		while( pq.size()>0 )
			buffer.add( pq.poll() ); 
		
		try{
			BufferedWriter bw = new BufferedWriter(new FileWriter( outputF1, false ) );
			for(int i=buffer.size()-1; i>=0; i--){
				bw.write( buffer.get(i).toString() );
				if(i>0)
					bw.newLine();
			}
			bw.flush();
			bw.close();
		}catch(IOException e){
		}
	}
	
	private void outputF2(){
		PriorityQueue<Resource> pq = new PriorityQueue<Resource>();
		for(Map.Entry<String, Long> e : resourceAccessedInfo.entrySet()){
			pq.add( new Resource( e.getKey(), e.getValue() ) );
			
			if(pq.size() > RESULT_SIZE)
				pq.poll();
		}
		List<Resource> buffer = new ArrayList<Resource>(RESULT_SIZE);
		while( pq.size()>0 )
			buffer.add( pq.poll() ); 
		
		try{
			BufferedWriter bw = new BufferedWriter(new FileWriter( outputF2, false ) );
			for(int i=buffer.size()-1; i>=0; i--){
				bw.write( buffer.get(i).toString() );
				if(i>0)
					bw.newLine();
			}
			bw.flush();
			bw.close();
		}catch(IOException e){
		}
	}
	
	private void outputF3(){
		for( Access ac : accessInfo.values() ){ //access remaining access information
			int bakCount = ac.getCount();
			ac.setCount( accessedCount );
			accessedCount -= bakCount;
			
			accessWindowsPQ.add(ac);
			if( accessWindowsPQ.size() > RESULT_SIZE )
				accessWindowsPQ.poll();
		}
		
		List<Access> buffer = new ArrayList<Access>(RESULT_SIZE);
		while( accessWindowsPQ.size()>0 )
			buffer.add( accessWindowsPQ.poll() ); 
		
		try{
			BufferedWriter bw = new BufferedWriter(new FileWriter( outputF3, false ) );
			for(int i=buffer.size()-1; i>=0; i--){
				bw.write( buffer.get(i).toString() );
				if(i>0)
					bw.newLine();
			}
			bw.flush();
			bw.close();
		}catch(IOException e){
		}
		//System.out.println(accessedCount);
	}
	
	private void outputF4(){
		try{
			BufferedWriter bw = new BufferedWriter(new FileWriter( outputF4, false ) );
			for(int i=0; i<blockedLog.size(); i++){
				bw.write( blockedLog.get(i) ); 
				if( i != blockedLog.size()-1 )
					bw.newLine();
			}
			bw.flush();
			bw.close();
		}catch(IOException e){
		}
	}
	
	public static void main(String[] args) throws Exception{
		if(args.length != 5){
			System.out.println("Usage: java  ProcessLog  $InputLogFile  $Output4Feature1  $Output4Feature2  $Output4Feature3  $Output4Feature4 \n");
			System.exit(1);
		}
		input = args[0];
		outputF1 = args[1];
		outputF2 = args[2];
		outputF3 = args[3];
		outputF4 = args[4];  
		
		long start = System.currentTimeMillis();
		ProcessLog pl = new ProcessLog();
		pl.process(input);        
		
		pl.outputF1();
		//System.out.println();
		
		pl.outputF2();
		//System.out.println();
		
		pl.outputF3();
		//System.out.println();
		
		pl.outputF4(); 
		//System.out.println();
		
		System.out.println( "Total execution seconds: " + (System.currentTimeMillis() - start)/1000 );
		//dateTest();
	}
	
	//only for internal testing
	private static void dateTest() throws Exception{
		DateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
		Date date = df.parse("01/Jul/1995:00:00:15 -0400");
		/*System.out.println(date.getYear());
		System.out.println(date.getMonth());
		System.out.println(date.getDate());
		System.out.println(date.getHours());
		System.out.println(date.getMinutes());
		System.out.println(date.getSeconds());
		System.out.println(date.getTimezoneOffset());*/
		Calendar cld = Calendar.getInstance();
		cld.setTime(date);
		System.out.println( cld.getTimeInMillis() );
		/*System.out.println( cld.get(Calendar.YEAR) );
		System.out.println( cld.get(Calendar.MONTH) );
		System.out.println( cld.get(Calendar.DAY_OF_MONTH) );
		System.out.println( cld.get(Calendar.HOUR_OF_DAY) );
		System.out.println( cld.get(Calendar.MINUTE) );
		System.out.println( cld.get(Calendar.SECOND) );
		System.out.println( cld.getTimeZone() ); */
		
		Calendar cld2 = (Calendar)cld.clone();
		//cld2.add(Calendar.YEAR, 1);
		//System.out.println( cld2.get(Calendar.YEAR) );
		System.out.println( cld2.getTimeInMillis() );
		
		Calendar cld3 = Calendar.getInstance();
		cld3.setTime( df.parse("01/Jul/1995:00:00:15 -0400") );
		System.out.println( cld3.getTimeInMillis() );
		System.out.println( df.format( cld3.getTime() ) );
	}
}
