import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;


public class Node extends Thread{

	private ServerSocket serverSocket;
	int port;
	private int seqNum = -1;
	private int N;
	List<Integer> list;

	private Socket leftSocket;
	private PrintWriter leftOut;
	private BufferedReader leftIn;

	private Socket rightSocket;
	private PrintWriter rightOut;
	private BufferedReader rightIn;

	private Socket masterSocket;
	private PrintWriter masterOut;
	private BufferedReader masterIn;

	public Node(int myPort, String masterHost, int masterPort){
		this.list = new ArrayList<Integer>();
		this.port = myPort;
		try {
			this.serverSocket = new ServerSocket(myPort);
			this.masterSocket = new Socket(masterHost, masterPort);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void run(){
		try{	
			String fromServer;

			// connect to master
			this.masterOut = new PrintWriter(masterSocket.getOutputStream(), true);
			this.masterIn = new BufferedReader(new InputStreamReader(masterSocket.getInputStream()));

			// tell master my port number and hostname
			this.masterOut.println(port + "");
			this.masterOut.println(masterSocket.getLocalAddress().getHostName());

			// get seqNum and N from master
			print("Getting seqNum and N from master");
			fromServer = masterIn.readLine();
			Scanner scan = new Scanner(fromServer);
			seqNum = scan.nextInt();
			N = scan.nextInt();
			scan.close();

			// get right node info (from master) if not last in sequence
			if(seqNum < N - 1){
				print("Getting right node info.");			
				fromServer = masterIn.readLine();
				scan = new Scanner(fromServer);	
				int port = scan.nextInt();
				String host = scan.next();
				scan.close();

				// connect to right node
				print("Connecting to right node.");
				this.rightSocket = new Socket(host, port);
				this.rightOut = new PrintWriter(rightSocket.getOutputStream(), true);
				this.rightIn = new BufferedReader(new InputStreamReader(rightSocket.getInputStream()));
				print("Connected!");
			}

			// connect to left node if not first in sequence
			if(seqNum != 0){
				print("Connecting to left node.");
				leftSocket = serverSocket.accept();
				this.leftOut = new PrintWriter(leftSocket.getOutputStream(), true);
				this.leftIn = new BufferedReader(new InputStreamReader(leftSocket.getInputStream()));
				print("Connected!");
			}

			// recieve array from master
			print("Receiving array...");
			fromServer = masterIn.readLine();
			scan = new Scanner(fromServer);
			while(scan.hasNext()){
				list.add(scan.nextInt());
			}
			print("Received: " + listToString());

			// start sorting woooo.  
			//---------------------
			Collections.sort(list);

			boolean swapHappened = false;
			boolean done = false;

			while(!done){
				// 1. the upwards pass
				//=====================
				
				// 1a. receive highest from left, send reply
				//------------------------------------------
				if(seqNum != 0){ // the first node has already passed up
					
					scan = new Scanner(leftIn.readLine());

					// check for exit signal
					if(!scan.hasNextInt()){ 
						print("1a. received done signal from node" + (seqNum - 1));
						done = true;
						if(seqNum < N-1){ // pass on if not at the end
							rightOut.println("d");
						}
						continue;
					}

					// receive bubbled up highest value from left
					int received = scan.nextInt();
					swapHappened = scan.nextBoolean();
					
					print(String.format("1a. received %d %b from node%d.", received, swapHappened, (seqNum-1)));
					
					// if will swap
					if(received > list.get(0)){ 
						swapHappened = true;
						int sent = list.remove(0);  // swap for your lowest
						leftOut.println(sent + ""); // send reply
							
						print(String.format("1b. sendng reply %d to node%d, swap.", sent, (seqNum-1)));
						
						list.add(received);
						Collections.sort(list);  // sort my array again
					}
					// otherwise no swap
					else {
						print(String.format("1b. sending reply %d to node%d, no swap.", received, (seqNum-1)));
						leftOut.println(received + ""); // send reply
					}
					
				}	
		
				// 1b. send highest right, receive reply
				//--------------------------------------
				if(seqNum < N - 1){ // all except the last node
					// send highest value right
					int sent = list.remove(list.size() - 1);
					rightOut.println(sent + " " + swapHappened);
					
					print(String.format("1c. sending %d %b to node%d, awaiting reply.", sent, swapHappened, (seqNum+1)));
					
					// receive lowest value reply from right
					scan = new Scanner(rightIn.readLine());
					int received = scan.nextInt();
					list.add(list.size()-1, received); // add to the end
					if(sent != received){
						Collections.sort(list); // sort if received a new value
					}
					
					print(String.format("1d. received %d from node%d.", received, (seqNum+1)));
				}

				// At the top - see if sorted
				//===========================
				// the node at the end can tell if complete!
				if(seqNum == N - 1 && !swapHappened){
					print("at the top, sort is complete. Sending message down.");
					done = true;
					leftOut.println("d");
					continue;
				}	
				
				// reset
				swapHappened = false;

				// 2. downwards pass
				//==================
				
				// 2a. receive lowest from right, send reply right
				//------------------------------------------------
				if(seqNum != N - 1){ // the last node has already passed down
					scan = new Scanner(rightIn.readLine());

					// check for exit signal
					if(!scan.hasNextInt()){ 
						print("2a. received done signal from node" + (seqNum+1));
						done = true;
						if(seqNum > 0){ // pass on if not the start node
							leftOut.println("d");
						}
						continue;
					}

					// receive lowest value from right
					int received = scan.nextInt();
					swapHappened = scan.nextBoolean();
					
					print(String.format("2a. received %d %b from node%d.", received, swapHappened, (seqNum+1)));

					// if will swap
					if(received < list.get(list.size()-1)){ 
						swapHappened = true;
						int sent = list.remove(list.size()-1); // swap for your highest
						rightOut.println(sent + ""); // send reply
						
						print(String.format("2b. sendng reply %d to node%d, swap.", sent, (seqNum+1)));
						
						list.add(received); 
						Collections.sort(list);  // sort my array again
					}
					// otherwise no swap
					else {
						rightOut.println(received + ""); // send reply
						print(String.format("2b. sending reply %d to node%d, no swap.", received, (seqNum+1)));
					}
				}
				
				// 2b. send lowest left and wait for a reply from left
				//----------------------------------------------------
				if(seqNum != 0){ // all except the first node
					// send lowest value left
					int sent = list.remove(0);
					leftOut.println(sent + " " + swapHappened);
					
					print(String.format("2c. sending %d %b to node%d, awaiting reply.", sent, swapHappened, (seqNum-1)));
					
					// receive highest value reply from left
					scan = new Scanner(leftIn.readLine());
					int received = scan.nextInt();
					list.add(0, received); // put at start
					if(sent != received){
						Collections.sort(list); // sort if we received a different value
					}
					
					print(String.format("2d. received %d from node%d.", received, (seqNum-1)));
				}
				
				
				// At the bottom - see if sorted
				//==============================
				// the node at the start can tell if complete!
				if(seqNum == 0 && !swapHappened){
					print("at the bottom, sort is complete. Sending message up.");
					done = true;
					rightOut.println("d");
					continue;
				}
				
				// reset
				swapHappened = false;
			}
			
			// print results for now
			print("Partial answer: " + listToString());
			
		} catch (Exception e){
			e.printStackTrace();
		} finally{
			try {
				// close everything
				serverSocket.close();
				if(leftSocket != null){
					leftSocket.close();
				}
				if(rightSocket != null){
					rightSocket.close();
				}
				masterSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void print(String s){
		if(seqNum == -1){
			System.out.println("node?: " + s);
		}
		else {
			System.out.println(String.format("node%d: ", seqNum) + s);
		}
	}

	private String listToString(){
		String s = "";
		for(Integer i: list){
			s += i + " ";
		}
		s = s.trim();
		return s;
	}

}
