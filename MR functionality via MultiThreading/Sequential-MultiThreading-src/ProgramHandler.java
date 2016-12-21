/*
 * ProgramHandler : 
 * 		- loads input file into ArrayList "csvArrayList"
 * 		- implements each version of sequential and multi-threaded programs 
 */


public class ProgramHandler{

	public static void main(String args[]) throws Exception {
		
		final String csvFile 		= "input/input.csv";
						
		CentralData c = new CentralData();

		System.out.println("Loading input.csv in memory...");
		
		// load "1912.csv" into memory
		c.loadInMemory(csvFile);
		
		System.out.println("\nLoading completed!!!");
		
		// perform each type of execution
		SequentialProcessing sequential = new SequentialProcessing();				
		CoarseLockProcessing coarseLock = new CoarseLockProcessing();				
		FineLockProcessing fineLock 	= new FineLockProcessing();				
		NoLockProcessing noLock 		= new NoLockProcessing();			
		NoSharingProcessing noSharing	= new NoSharingProcessing();
		
		System.out.println("\nTime Logs for Sequential : ");
		sequential.SequentialExecution(c);
		
		System.out.println("\nTime Logs for Coarse Lock : ");
		coarseLock.CoarseLockExecution(c);
		
		System.out.println("\nTime Logs for Fine Lock : ");
		fineLock.FineLockExecution(c);
		
		System.out.println("\nTime Logs for No Lock : ");
		noLock.NoLockExecution(c);
		
		System.out.println("\nTime Logs for No Sharing : ");
		noSharing.NoSharingExecution(c);
		
		// perform each type of execution with delay fibo(17)
		SequentialProcessing_Fibo sequential_f 	= new SequentialProcessing_Fibo();				
		CoarseLockProcessing_Fibo coarseLock_f 	= new CoarseLockProcessing_Fibo();				
		FineLockProcessing_Fibo fineLock_f 		= new FineLockProcessing_Fibo();				
		NoLockProcessing_Fibo noLock_f 			= new NoLockProcessing_Fibo();			
		NoSharingProcessing_Fibo noSharing_f	= new NoSharingProcessing_Fibo();
		
		System.out.println("\nTime Logs for Sequential with delay : ");
		sequential_f.SequentialExecution_Fibo(c);
		
		System.out.println("\nTime Logs for Coarse Lock with delay : ");
		coarseLock_f.CoarseLockExecution_Fibo(c);
		
		System.out.println("\nTime Logs for Fine Lock with delay : ");
		fineLock_f.FineLockExecution_Fibo(c);;
		
		System.out.println("\nTime Logs for No Lock with delay : ");
		noLock_f.NoLockExecution_Fibo(c);
		
		System.out.println("\nTime Logs for No Sharing with delay : ");
		noSharing_f.NoSharingExecution_Fibo(c);
	}
}