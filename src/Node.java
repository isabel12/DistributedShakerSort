import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


public class Node extends Thread implements Comparable<Node>{
	private boolean oneThreadOnly;
	private boolean debuggingOn = true;
	Map<String, List<String>> sortMessages = new HashMap<String, List<String>>();

	// my 'letterbox'
	private ServerSocket serverSocket;
	private ServerSocket serverSocket2;
	int port;
	int port2;

	private int seqNum = -1;
	private int N;
	List<Integer> list;

	// channels
	String masterHost;
	int masterPort;
	private Socket masterSocket;
	private PrintWriter masterOut;
	private BufferedReader masterIn;

	// for coordinating which thread has permission
	AtomicBoolean avail = new AtomicBoolean(true);
	Semaphore waitForPermission = new Semaphore(0);

	/**
	 * Constructor 
	 * @param myPort
	 * @param masterHost
	 * @param masterPort
	 */
	public Node(int myPort, int myPort2, String masterHost, int masterPort, boolean debuggingOn, boolean oneThreadOnly){
		this.oneThreadOnly = oneThreadOnly;
		this.masterHost = masterHost;
		this.masterPort = masterPort;
		this.debuggingOn = debuggingOn;
		this.list = new ArrayList<Integer>();
		this.port = myPort;
		this.port2 = myPort2;
		try {
			// create serverSocket
			this.serverSocket = new ServerSocket(myPort);
			this.serverSocket2 = new ServerSocket(myPort2);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * This method connects to master, sets up for the sort, performs the sort, and returns the results to master.
	 */
	public void run(){
		try{	
			String fromServer;

			// connect to master
			this.masterSocket = new Socket(masterHost, masterPort);
			this.masterOut = new PrintWriter(masterSocket.getOutputStream(), true);
			this.masterIn = new BufferedReader(new InputStreamReader(masterSocket.getInputStream()));

			// tell master my port numbers and hostname
			this.masterOut.println(port + "");
			this.masterOut.println(port2 + "");
			this.masterOut.println(masterSocket.getLocalAddress().getHostName());

			// get seqNum and N from master
			print("Getting seqNum and N from master");
			fromServer = masterIn.readLine();
			Scanner scan = new Scanner(fromServer);
			seqNum = scan.nextInt();
			N = scan.nextInt();
			scan.close();

			// initialise the sortMessages lists
			sortMessages.put(String.format("t1node%d: ", seqNum), new ArrayList<String>());
			sortMessages.put(String.format("t2node%d: ", seqNum), new ArrayList<String>());

			// get right node info (from master) if not last in sequence
			int rightPort = -1;
			int rightPort2 = -1;
			String rightHost = null;
			if(seqNum != N - 1){
				print("Getting right node info from master.");			
				fromServer = masterIn.readLine();
				scan = new Scanner(fromServer);	
				rightPort = scan.nextInt();
				rightPort2 = scan.nextInt();
				print(String.format("myPort: %d, myPort2: %d, rightPort: %d, rightPort2: %d", port, port2, rightPort, rightPort2));
				rightHost = scan.next();
				scan.close();
			}


			// get end node info (from master) if are first in sequence
			int endPort = -1;
			int endPort2 = -1;
			String endHost = null;
			if(seqNum == 0){
				print("Getting end node info from master.");			
				fromServer = masterIn.readLine();
				scan = new Scanner(fromServer);	
				endPort = scan.nextInt();
				endPort2 = scan.nextInt();
				print(String.format("myPort: %d, myPort2: %d, endPort: %d, endPort2: %d", port, port2, endPort, endPort2));
				endHost = scan.next();
				scan.close();	
			}


			// recieve array from master
			print("Receiving array...");
			fromServer = masterIn.readLine();
			scan = new Scanner(fromServer);
			while(scan.hasNext()){
				list.add(Integer.parseInt(scan.next()));
			}
			print("Received: " + listToString());

			// start sorting woooo.  
			//---------------------			
			long startTime = System.currentTimeMillis();	

			Collections.sort(list);

			// create the search threads
			SortThread search1 = new SortThread(false, serverSocket, rightPort, rightHost, endPort, endHost);
			SortThread search2 = null;
			if(!oneThreadOnly){
				search2 = new SortThread(true, serverSocket2, rightPort2, rightHost, endPort2, endHost);
			}
			
			// start search, and wait
			search1.start();
			if(!oneThreadOnly){
				search2.start();
			}
			search1.join();	
			if(!oneThreadOnly){
				search2.join();
			}
			
			long timeTaken = System.currentTimeMillis() - startTime;

			// print results for now
			print("Partial answer: " + listToString());

			// connect to master again (close previous connections first)
			this.masterOut.close();
			this.masterIn.close();
			this.masterSocket = new Socket(masterHost, masterPort);
			this.masterOut = new PrintWriter(masterSocket.getOutputStream(), true);
			this.masterIn = new BufferedReader(new InputStreamReader(masterSocket.getInputStream()));

			// tell master the results, and how long it took
			this.masterOut.println(seqNum);
			this.masterOut.println(listToString());
			this.masterOut.println(timeTaken);

			// wait for master to say all done
			this.masterIn.readLine();
			
			// close the connections
			search1.closeConnections();
			if(!oneThreadOnly){
				search2.closeConnections();
			}
			
		} catch (Exception e){
			e.printStackTrace();
		} finally{
			try {
				// close everything
				serverSocket.close();
				serverSocket2.close();
				masterSocket.close();
				masterIn.close();
				masterOut.close();		
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}


	/**
	 * Internal SortThread class. Two instances are created per node. 
	 * Each SortThread instance belongs one of two different chains of SortThreads - one that starts at the top, and one that starts at the bottom.
	 * 
	 * These chains search concurrently, going in opposite directions, and waiting at the top for the other to finish before going back the other way.
	 * 
	 * At each end, they then check whether they know the sort done, tell their neighbour, and also the other end of the other 'SortThread chain'. 
	 * If they are not done, they wait for the other chain to get to the other end, check whether the other chain is done, and if so tell their neighbour.
	 * Otherwise, if neither 'chain' thinks the sort is done, they both start going back the other way.
	 * 
	 * Only one SearchThread per node can be active at once.  However, because the sorts will cross meet in the middle somewhere, the first SearchThread to get access
	 * to the node gets it, and the other SortThread realises the Node permission isn't available, calls the previous SortThread in its chain to release permissions and redo its last action.  
	 * This effectively means the Thread that came second gives way to the first, and prevents deadlock.
	 * 
	 * @author Izzi
	 *
	 */
	private class SortThread extends Thread{
		
		boolean startAtTop;
		ServerSocket serverSocket;
		int rightPort;
		String rightHost;
		int endPort;
		String endHost;

		boolean firstRound = true;

		// channels
		private Socket leftSocket;
		private PrintWriter leftOut;
		private BufferedReader leftIn;

		private Socket rightSocket;
		private PrintWriter rightOut;
		private BufferedReader rightIn;

		private Socket endSocket;
		private PrintWriter endOut;
		private BufferedReader endIn;


		public SortThread(boolean startAtTop, ServerSocket serverSocket, int rightPort, String rightHost, int endPort, String endHost) throws IOException{			
			this.startAtTop = startAtTop;
			this.serverSocket = serverSocket;
			this.rightPort = rightPort;
			this.rightHost = rightHost;
			this.endPort = endPort;
			this.endHost = endHost;
		}

		public void run(){
			Scanner scan;

			try{	
				//  if not last in sequence, connect to right node
				if(seqNum < N - 1){
					print("Connecting to right node.", startAtTop);
					this.rightSocket = new Socket(rightHost, rightPort);
					this.rightOut = new PrintWriter(rightSocket.getOutputStream(), true);
					this.rightIn = new BufferedReader(new InputStreamReader(rightSocket.getInputStream()));
					print("Connected to right!", startAtTop);
				}

				// if not first in sequence, connect to left node if not first in sequence
				if(seqNum != 0){
					print("Connecting to left node.", startAtTop);
					leftSocket = serverSocket.accept();
					this.leftOut = new PrintWriter(leftSocket.getOutputStream(), true);
					this.leftIn = new BufferedReader(new InputStreamReader(leftSocket.getInputStream()));
					print("Connected to left!", startAtTop);
				}

				if(!oneThreadOnly){
					// if first in sequence, connect to end
					if(seqNum == 0){
						print("Connecting to end node of other thread.", startAtTop);
						this.endSocket = new Socket(endHost, endPort);
						this.endOut = new PrintWriter(endSocket.getOutputStream(), true);
						this.endIn = new BufferedReader(new InputStreamReader(endSocket.getInputStream()));
						print("Connected to end!", startAtTop);
					}
	
					// if last in sequence, connect to start
					if(seqNum == N - 1){
						print("Connecting to start node of other thread.", startAtTop);
						this.endSocket = serverSocket.accept();
						this.endOut = new PrintWriter(endSocket.getOutputStream(), true);
						this.endIn = new BufferedReader(new InputStreamReader(endSocket.getInputStream()));
						print("Connected to start!", startAtTop);	
					}
				}

				print("sort method started.", startAtTop);

				boolean swapHappened = false;

				if(!(leftSocket == null && rightSocket == null)){ // skip this if N = 1
					while(true){
						// 1. the upwards pass
						//=====================

						if(!firstRound || (firstRound && !startAtTop)){

							// 1a,b. receive highest from left, send reply
							//------------------------------------------			
							if(seqNum != 0){ // the first node has already passed up

								int received;
								while(true){

									print("1a. receiving from left...", startAtTop);
									scan = new Scanner(leftIn.readLine());

									// check for exit signal
									if(scan.hasNext("d")){ 
										print("1a. received done signal from node" + (seqNum - 1), startAtTop);
										if(seqNum < N-1){ // pass on if not at the end
											rightOut.println("d");
										} 
										//print("returning.", startAtTop);
										return;
									}

									// receive bubbled up highest value from left
									received = scan.nextInt();
									swapHappened = scan.nextBoolean();
									print(String.format("1a. received %d %b from node%d.", received, swapHappened, (seqNum-1)), startAtTop);

									// check if need to give way to other thread (this will acquire permission if it is available)
									print("acquiring sem.", startAtTop);
									if(!avail.getAndSet(false)){							
										// send a reset signal
										print("permission not available, resetting.", startAtTop);
										leftOut.println("r");
									} else {
										print("acquired sem.", startAtTop);
										break;		
									}
								}

								// if will swap
								if(received > list.get(0)){ 

									swapHappened = true;
									int sent = list.remove(0);  // swap for your lowest
									leftOut.println(sent + ""); // send reply

									print(String.format("1b. sendng reply %d to node%d, swap.", sent, (seqNum-1)), startAtTop);

									list.add(received);
									Collections.sort(list);  // sort my array again

								}
								// otherwise no swap
								else {
									print(String.format("1b. sending reply %d to node%d, no swap.", received, (seqNum-1)), startAtTop);
									leftOut.println(received + ""); // send reply
								}
								release();
							}	

							// 1c,d. send highest right, receive reply
							//--------------------------------------	
							if(seqNum < N - 1){ // all except the last node
								acquire();
								int sent;
								while(true){

									// remove highest value
									sent = list.remove(list.size() - 1);	

									// send highest value right
									print(String.format("1c. sending %d %b to node%d, awaiting reply.", sent, swapHappened, (seqNum+1)), startAtTop);
									rightOut.println(sent + " " + swapHappened);

									// receive lowest value reply from right
									print(String.format("1d. awaiting lowest value reply from right..."), startAtTop);
									scan = new Scanner(rightIn.readLine());

									// if reset, reverse and go to back of permission 'queue'
									if(scan.hasNext("r")){
										print("reset received.", startAtTop);
										list.add(sent);	
										release();
										Random r = new Random();
										Thread.sleep(r.nextInt(10));
										acquire();
									} else{
										break;
									}	
								}

								int received = scan.nextInt();
								print(String.format("1d. received %d from node%d.", received, (seqNum+1)), startAtTop);

								if(list.isEmpty()){
									list.add(received);
								} else {
									list.add(list.size(), received); // add to the end
								}
								if(sent != received){
									Collections.sort(list); // sort if received a new value
								}	

								release();
							}


							// At the top - see if sorted
							//===========================
														
							// the node at the end can tell if complete!
							if(seqNum == N - 1 && !swapHappened){
								print("at the top, sort is complete. Sending message down.", startAtTop);
								leftOut.println("d");
								// wake up other thread
								if(!oneThreadOnly && (seqNum == 0 || seqNum == N-1)){ 
									endOut.println("d");
								} 
								return;
							}	

							if(!oneThreadOnly){
								// wait for other thread to finish before you switch
								if(seqNum == N - 1){															
									// wait for other thread to finish
									print("waiting for other thread to finish.", startAtTop);
									endOut.println("n");
									String done = endIn.readLine();
									print("woke up.", startAtTop);
	
									// check if done, send message down
									if(done.startsWith("d")){
										print("done.", startAtTop);
										leftOut.println("d");
										return;
									}
									print("not done.", startAtTop);	
								}
							}

							// reset
							swapHappened = false;
							firstRound = false;
						}

						if(!firstRound || (firstRound && startAtTop)){ // makes sure that if startAtTop is true, will start here
							// 2. downwards pass
							//==================

							// 2a. receive lowest from right, send reply right
							//------------------------------------------------
							if(seqNum != N - 1){ // the last node has already passed down							

								int received;
								while(true){

									print("2a. receiving from right...", startAtTop);
									scan = new Scanner(rightIn.readLine());

									// check for exit signal
									if(scan.hasNext("d")){ 
										print("2a. received done signal from node" + (seqNum+1), startAtTop);
										if(seqNum > 0){ // pass on if not the start node
											leftOut.println("d");
										} 
										return;
									}

									// receive lowest value from right
									received = scan.nextInt();
									swapHappened = scan.nextBoolean();
									print(String.format("2a. received %d %b from node%d.", received, swapHappened, (seqNum+1)), startAtTop);

									// check if need to give way to other thread (this will acquire permission if it is available)
									print("acquiring sem.", startAtTop);
									if(!avail.getAndSet(false)){							
										// send a reset signal
										print("permission not available, resetting.", startAtTop);
										rightOut.println("r");
									} else {
										print("acquired sem.", startAtTop);
										break;		
									}
								}


								// if will swap								
								if(received < list.get(list.size()-1)){ 
									swapHappened = true;
									int sent = list.remove(list.size()-1); // swap for your highest
									rightOut.println(sent + ""); // send reply

									print(String.format("2b. sendng reply %d to node%d, swap.", sent, (seqNum+1)), startAtTop);

									list.add(received); 
									Collections.sort(list);  // sort my array again
								}
								// otherwise no swap
								else {
									rightOut.println(received + ""); // send reply
									print(String.format("2b. sending reply %d to node%d, no swap.", received, (seqNum+1)), startAtTop);
								}
								release();
							}

							// 2c,d. send lowest left and wait for a reply from left
							//----------------------------------------------------
							if(seqNum != 0){ // all except the first node
								acquire();
								int sent;
								while(true){
									// remove lowest value
									sent = list.remove(0);

									// send lowest value left
									print(String.format("2c. sending %d %b to node%d, awaiting reply.", sent, swapHappened, (seqNum-1)), startAtTop);
									leftOut.println(sent + " " + swapHappened);							

									// receive highest value reply from left
									print(String.format("2d. awaiting highest value reply from left..."), startAtTop);
									scan = new Scanner(leftIn.readLine());

									// if reset, reverse and go to back of permission 'queue'
									if(scan.hasNext("r")){
										print("reset received.", startAtTop);
										list.add(0, sent);	
										release();
										Random r = new Random();
										Thread.sleep(r.nextInt(10));
										acquire();
									} else{
										break;
									}	
								}	

								int received = scan.nextInt();
								list.add(0, received); // put at start
								if(sent != received){
									Collections.sort(list); // sort if we received a different value
								}

								print(String.format("2d. received %d from node%d.", received, (seqNum-1)), startAtTop);

								release();
							}


							// At the bottom - see if sorted
							//==============================
							
							// the node at the start can tell if complete!
							if(seqNum == 0 && !swapHappened){
								print("at the bottom, sort is complete. Sending message up.", startAtTop);
								rightOut.println("d");
								// wake up other thread
								if(!oneThreadOnly && (seqNum == 0 || seqNum == N-1)){  
									endOut.println("d");
								} 
								
								return;
							}

							if(!oneThreadOnly){
								// wait for other thread to finish before you switch
								if(seqNum == 0){															
									// wait for other thread to finish
									print("waiting for other thread to finish.", startAtTop);
									endOut.println("n"); // say not done
									String done = endIn.readLine();
									print("woke up.", startAtTop);
	
									// check if done, send message up
									if(done.startsWith("d")){
										print("done.", startAtTop);
										rightOut.println("d");	
										return;
									}
									print("not done.", startAtTop);								
								}
							}

							// reset
							swapHappened = false;
							firstRound = false;
						}
					}
				}
			} catch(Exception e){
				e.printStackTrace();
			} 
		}

		/**
		 * This method makes sure that connections aren't closed prematurely when the thread exits the run() method.
		 */
		public void closeConnections(){
			try {
				// close everything
				if(leftSocket != null){
					print("closing left socket.", startAtTop );
					leftSocket.close();
					leftIn.close();
					leftOut.close();
				}
				if(rightSocket != null){
					print("closing right socket.", startAtTop );
					rightSocket.close();
					rightIn.close();
					rightOut.close();
				}
				if(!oneThreadOnly && endSocket != null){
					print("closing end socket.", startAtTop );
					endSocket.close();
					endIn.close();
					endOut.close();		
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		/**
		 * Acquires the permission to be active in the node, or blocks if not available.
		 * 
		 * @throws InterruptedException
		 */
		private void acquire() throws InterruptedException {
			print("acquiring sem.", startAtTop);
			if(!avail.getAndSet(false)){
				waitForPermission.acquire();
			}
			print("acquired sem.", startAtTop);	
		}

		/**
		 * Releases the permission to be active in the node, and wakes the other thread if it is waiting.
		 * 
		 * @throws InterruptedException
		 */
		private void release(){
			print("releasing sem.", startAtTop);
			avail.set(true);
			waitForPermission.release();
		}

	}	

	/**
	 * A method to print debugging messages to the screen - will do nothing if the 'debuggingOn' field is false.
	 * @param s
	 */
	private void print(String s){
		if(!debuggingOn){
			return;
		}

		if(seqNum == -1){
			System.out.println("node?: " + s);
		}
		else {
			System.out.println(String.format("node%d: ", seqNum) + s);
		}
	}

	/**
	 * Print method for the different search threads to differentiate their messages - will do nothing if the 'debuggingOn' field is false.
	 * @param s
	 * @param startAtTop
	 */
	private void print(String s, boolean startAtTop){
		if(debuggingOn){ 
			String key = String.format("t%dnode%d: ", startAtTop ? 2 : 1, seqNum );
			System.out.println(key + s);
			sortMessages.get(key).add(s);
		}
	}

	/**
	 * Method the Main can call to print debugging statements for SearchThread1
	 */
	public void printDebuggingForThread1(){		
		// print messages in order
		String key = String.format("t1node%d: ", seqNum);
		for(String s: sortMessages.get(key)){
			System.out.println(key + s);
		}
	}

	/**
	 * Method the Main can call to print debugging statements for SearchThread2
	 */
	public void printDebuggingForThread2(){
		// print messages in order
		String key = String.format("t2node%d: ", seqNum);
		for(String s: sortMessages.get(key)){
			System.out.println(key + s);
		}	
	}

	/**
	 * Converts the current list to a String representation.
	 * eg. "1 2 3 4 5 "
	 * @return
	 */
	private String listToString(){
		String s = "";
		for(Integer i: list){
			s += i + " ";
		}
		return s;
	}

	@Override
	public int compareTo(Node arg0) {
		return this.seqNum - arg0.seqNum;
	}


}
