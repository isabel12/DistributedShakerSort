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
	private int last;
	private int first = 0;

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
			// set handy dandy variables for list index positions
			first = 0;
			last = list.size() - 1;


			// start sorting woooo.  
			//---------------------
			Collections.sort(list);


			boolean swapHappened = false;
			boolean done = false;

			// first node starts it off
			if(seqNum == 0){
				int sent = list.remove(last);
				rightOut.println(sent + " " + swapHappened);
			}
			
			
			while(!done){
				// upwards pass
				//-------------
				if(seqNum != 0){ // the first node has already passed up
					scan = new Scanner(leftIn.readLine());

					// check for exit signal, and pass on if so
					if(!scan.hasNextInt()){ 
						done = true;
						rightOut.println("d");
						break;
					}

					// get the bubbled up highest value
					int received = scan.nextInt();
					swapHappened = scan.nextBoolean();
					list.add(received);
					Collections.sort(list);

					// swap it for your lowest
					int sent = list.remove(first);
					leftOut.println(sent + "");

					// the node at the end can tell if complete!
					if(seqNum == N - 1 && !swapHappened){
						done = true;
						leftOut.println("d");
						break;
					}

				}

				// downwards pass
				//---------------
				if(seqNum != N - 1){ // the last node has already passed down
					scan = new Scanner(rightIn.readLine());

					// check for exit signal, and pass on if so
					if(!scan.hasNextInt()){ 
						done = true;
						leftOut.println("d");
						break;
					}

					// get the bubbled down lowest value
					int received = scan.nextInt();
					swapHappened = scan.nextBoolean();
					list.add(received);
					Collections.sort(list);

					// swap it for your lowest
					int sent = list.remove(last);
					leftOut.println(sent + "");

					// the node at the start can tell if complete!
					if(seqNum == 0 && !swapHappened){
						done = true;
						rightOut.println("d");
						break;
					}
				}
			}

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
