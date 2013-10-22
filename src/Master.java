import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;


public class Master extends Thread{
	boolean debuggingOn;
	
	// my portnumber
	int portnumber = 4444;
	ServerSocket serverSocket;
	
	// results fields
	String[] nodeResults;
	String resultString;
	int[] result;	
	long startTime;
	long timeTakenMillis;
	long longestNodeTime;
	
	// fields for sort
	int N = 0;
	int[] toSort;
	
	
	// channels and info about nodes
	Socket[] nodeSockets;

	PrintWriter[] nodeOut;
	BufferedReader[] nodeIn; 
	
	int[] nodePortnumbers;
	int[] nodePortnumbers2;
	String[] nodeHostnames;

	
	/**
	 * @param n - must be less than or equal to the size of the 'toSort' array.
	 * @param portnumber
	 * @param toSort
	 */
	public Master(int n, int portnumber, int[] toSort, boolean debuggingOn){
		if(n > toSort.length){
			throw new IllegalArgumentException("n must be less than or equal to the size of the 'toSort' array.");
		}
			
		this.debuggingOn = debuggingOn;
		this.toSort = toSort;
		this.N = n;
		this.result = new int[N];
		this.nodeResults = new String[N];
		this.portnumber = portnumber;
		this.nodeSockets = new Socket[N];
		this.nodeOut = new PrintWriter[N];
		this.nodeIn = new BufferedReader[N];
		this.nodePortnumbers = new int[N];
		this.nodePortnumbers2 = new int[N];
		this.nodeHostnames = new String[N];
	}
	
	public void run(){
		try{
			serverSocket = new ServerSocket(portnumber);
			
			// receive node connections
			int numConnected = 0;
			while(numConnected < N){
				print("Waiting for " + (N - numConnected) + " nodes to connect...");
				nodeSockets[numConnected] = serverSocket.accept();
				nodeOut[numConnected] = new PrintWriter(nodeSockets[numConnected].getOutputStream(), true);
				nodeIn[numConnected] = new BufferedReader(new InputStreamReader(nodeSockets[numConnected].getInputStream()));
				
				nodePortnumbers[numConnected] = Integer.parseInt(nodeIn[numConnected].readLine()); // wait for the node to tell me their serverSocket portnumber
				nodePortnumbers2[numConnected] = Integer.parseInt(nodeIn[numConnected].readLine()); // wait for the node to tell me their serverSocket portnumber2
				nodeHostnames[numConnected] = nodeIn[numConnected].readLine().trim(); // wait for the node to tell me their hostname
				numConnected++;
				print("Received connection!");
			}
			
			// tell all nodes about their seqNum and N
			for(int sNum = 0; sNum < N; sNum++){
				String toSend = String.format("%d %d", sNum, N);
				nodeOut[sNum].println(toSend);
			}
			
			// tell all nodes about right neighbours
			for(int i = 0; i < N - 1; i++){
				print("Telling node " + i + " about right neighbour.");
				String toSend = String.format("%d %d %s", nodePortnumbers[i+1], nodePortnumbers2[i+1], nodeHostnames[i+1]);
				nodeOut[i].println(toSend);
			}
			
			// initiate sort
			sort();
				
			// wait for results		
			int numResults = 0;
			while(numResults < N){
				print("Waiting for " + (N - numResults) + " nodes to return results...");
				Socket socket = serverSocket.accept();
				BufferedReader nodeIn = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				PrintWriter nodeOut = new PrintWriter(socket.getOutputStream(), true);
				
				
				// find out which node it is
				int seqNum = Integer.parseInt(nodeIn.readLine());
				
				// get their result
				String result = nodeIn.readLine();
				nodeResults[seqNum] = result;
				
				// get how long it took
				long time = Long.parseLong(nodeIn.readLine());
				if(time > longestNodeTime){
					longestNodeTime = time;
				}
						
				// send reply
				nodeOut.println();
				
				numResults++;
				print("Received result from node" + seqNum);

			}
			
			// compile results
			resultString = "";
			for(int i = 0; i < N; i++){
				resultString += nodeResults[i];
			}
			
			print("sorted array: " + resultString);
			
			Scanner scan = new Scanner(resultString);
			int index = 0;
			while(scan.hasNext()){
				this.result[index] = Integer.parseInt(scan.next());
			}
			
			timeTakenMillis = System.currentTimeMillis() - startTime;				
			
		} catch (Exception e){
			e.printStackTrace();
			System.exit(0);
		} finally{
			try {
				serverSocket.close();
				for(Socket s: nodeSockets){
					s.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Time taken to complete the sort from master's perspective.
	 * @return
	 */
	public long getTimeTaken(){
		return this.timeTakenMillis;
	}
	
	/**
	 * The longest recorded time taken for any node to perform only the 'sort' section of code.
	 * @return
	 */
	public long getLongestNodeTime(){
		return this.longestNodeTime;
	}
	
	/**
	 * Returns the sorted array.
	 * @return
	 */
	public int[] getResultArray(){
		return this.result;
	}
	
	/**
	 * Returns a String representation of the sorted array.
	 * @return
	 */
	public String getResultString(){
		return this.resultString;
	}
	
	/**
	 * Initiates the sort.  First splits the array, then sends each section to each node.
	 */
	private void sort(){
		print("Initiating sort...");
		
		int[] nodeSizes = new int[N];
		
		// work out size of all nodes
		int minSize = toSort.length/N;
		int leftover = toSort.length - minSize*N;
		for(int i = 0; i < N; i++){
			nodeSizes[i] = minSize;
			if(i < leftover){
				nodeSizes[i]++;
			}
		}
		
		startTime = System.currentTimeMillis();
		
		// pass each node their bit of the array
		int p = 0;
		int end = 0;
		for(int i = 0; i < N; i++){
			// work out what section we are sending		
			p = end;
			end += nodeSizes[i];
			
			// make the string
			String array = "";
			while(p < end){
				array += toSort[p++] + " ";				
			}
			
			// send it
			nodeOut[i].println(array);
		}
	}
	
	/**
	 * Helper method to print debugging messages to console.  Does nothing if debuggingOn is false.
	 * @param s
	 */
	private void print(String s){	
		if(debuggingOn){
			System.out.println("master: " + s);
		}
	}
		
}
