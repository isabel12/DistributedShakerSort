import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;


public class Master extends Thread{

	ServerSocket serverSocket;
	int[] toSort;
	int N = 0;
	int portnumber = 4444;
	
	Socket[] nodeSockets;
	int[] nodePortnumbers;
	String[] nodeHostnames;
	PrintWriter[] nodeOut;
	BufferedReader[] nodeIn; 
	
	
	public Master(int n, int portnumber, int[] toSort){
		this.toSort = toSort;
		this.N = n;
		this.portnumber = portnumber;
		this.nodeSockets = new Socket[N];
		this.nodeOut = new PrintWriter[N];
		this.nodeIn = new BufferedReader[N];
		this.nodePortnumbers = new int[N];
		this.nodeHostnames = new String[N];
	}
	
	public void run(){
		try{
			serverSocket = new ServerSocket(portnumber);
			
			// receive node connections
			int numConnected = 0;
			while(numConnected < N){
				System.out.println("Waiting for " + (N - numConnected) + " nodes to connect...");
				nodeSockets[numConnected] = serverSocket.accept();
				nodeOut[numConnected] = new PrintWriter(nodeSockets[numConnected].getOutputStream(), true);
				nodeIn[numConnected] = new BufferedReader(new InputStreamReader(nodeSockets[numConnected].getInputStream()));
				nodePortnumbers[numConnected] = Integer.parseInt(nodeIn[numConnected].readLine()); // wait for the node to tell me their serverSocket portnumber
				nodeHostnames[numConnected] = nodeIn[numConnected].readLine().trim(); // wait for the node to tell me their hostname
				numConnected++;
				System.out.println("Received connection!");
			}
			
			// tell all nodes about their seqNum and N
			for(int sNum = 0; sNum < N; sNum++){
				String toSend = String.format("%d %d", sNum, N);
				nodeOut[sNum].println(toSend);
			}
			
			// tell all nodes about right neighbours
			for(int i = 0; i < N - 1; i++){
				System.out.println("Telling node " + i + " about right neighbour.");
				String toSend = String.format("%d %s", nodePortnumbers[i+1], nodeHostnames[i+1]);
				nodeOut[i].println(toSend);
			}
			
			// initiate sort
			sort();
			
			
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
	
	
	public void sort(){
		System.out.println("Initiating sort");
		
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
			array.trim();
			
			// send it
			nodeOut[i].println(array);
		}

		
	}
		
}
