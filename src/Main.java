import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

/**
 * This class is the entry point into the program.  It creates the master and nodes, starts them, waits for master to complete, then prints results.
 * 
 * It also runs multiple tests.
 * 
 * @author Izzi
 *
 */
public class Main {

	private static final int masterPort = 4444;	

	// where results are saved
	private static long timeTakenTotal;// from master's prespective (i.e. including sending and receiving array)
	private static long longestNodeTime; // from node's perspective


	/**
	 * This method coordinates and initiates the sort.  It creates the master, creates the nodes, and starts them all. Results are saved in timeTakenTotal and longestNodeTime fields.
	 */
	private static void start(int[] toSort, int N, boolean debuggingOn, boolean onlyOneThread){	

		// get master hostname
		String masterHostname = "";
		try {
			masterHostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

		// start the master
		Master master = new Master(N, masterPort, toSort, debuggingOn);
		master.start();

		// create and start the nodes
		int port = masterPort;
		Node[] nodes = new Node[N];
		for(int i = 0; i < N; i++){
			nodes[i] = new Node(++port, ++port, masterHostname, masterPort, debuggingOn, onlyOneThread);
			nodes[i].start();
		}

		// wait for master to finish and get result
		try {

			// print ordered debugging statements every 5 secs.
			if(debuggingOn){
				while(master.isAlive()){
					Thread.sleep(5000);

					Arrays.sort(nodes);

					System.out.println();
					System.out.println("Printing debugging statements by node");
					System.out.println("-------------------------------------");
					for(Node n: nodes){
						n.printDebuggingForThread1();
						System.out.println();
					}
					System.out.println();
					for(Node n: nodes){
						n.printDebuggingForThread2();
						System.out.println();
					}		
				}
			} else {
				master.join();
			}

			Main.timeTakenTotal = master.getTimeTaken();
			Main.longestNodeTime = master.getLongestNodeTime();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * This is a helper method to generate a random array of given size.
	 * @param size
	 * @return
	 */
	private static int[] generateRandomArray(int size){
		Random r = new Random();
		int[] array = new int[size];	
		int max = size > Integer.MAX_VALUE? Integer.MAX_VALUE: size;	
		for(int i = 0; i < size; i++){
			array[i] = r.nextInt(max);
		}	
		return array;
	}

	/**
	 * This method runs a number of tests, printing out the arraysize, N, and time taken total.
	 * @param arraySize
	 * @param sizesOfNToTest
	 */
	private static void runTest(int arraySize, int[] sizesOfNToTest, int numRepeats, boolean debuggingOn, boolean onlyOneThread){
		// generate array
		int[] toSort = generateRandomArray(arraySize);

		System.out.println("Initiating test");
		System.out.println("Array size: " + toSort.length);
		System.out.println("==================");

//		// do collections.sort()
//		List<Integer> copy = new ArrayList<Integer>();
//		for(int i: toSort){
//			copy.add(i);
//		}
//		long start = System.currentTimeMillis();
//		Collections.sort(copy);
//		long timeTaken = System.currentTimeMillis() - start;
//		System.out.print("Collections.sort(): ");
//		System.out.println(timeTaken + "ms\r\n");

		// do tests and average results
		long averageTotal = 0;
		long averageNode = 0;
		for(int N: sizesOfNToTest){

			System.out.println("N: " + N);
			System.out.println(String.format("%d threads", onlyOneThread?1:2));

			for(int i = 0; i < numRepeats; i++){

				start(toSort, N, debuggingOn, onlyOneThread);
				averageTotal += timeTakenTotal;
				averageNode += longestNodeTime;	
			}
			averageTotal = averageTotal / numRepeats;
			averageNode = averageNode / numRepeats;


			System.out.println("array sending: " + (averageTotal - averageNode) + "ms");
			System.out.println("sort time: " + averageNode + "ms");
			System.out.println("total: " + averageTotal + "ms");
			System.out.println();

			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * The main method.  The order of the arguments doesn't matter, and all are optional.  
	 * If they aren't specified, defaults will be used.  If no arguments are given, all will be asked for.
	 * 
	 * Argument formats are:
	 * "arraySize=<int>"
	 * "N=<int>"
	 * "numRepeats=<int>"
	 * "debuggingOn=<boolean>"
	 * 
	 * @param args
	 */
	public static void main(String[] args){

		// defaults
		int[] arraySize = new int[]{5};
		int[] NToTest = {3};
		int numRepeats = 1;
		boolean debuggingOn = false;
		boolean oneThreadOnly = false;

		try{
			// parse arguments if there are any
			if(args.length > 0){		
				for(int i = 0; i < args.length; i++){

					String arg = args[i].toLowerCase();

					if(arg.startsWith("arraysize=")){
						arg = arg.replace("arraysize=", "");
						arraySize[0] = Integer.parseInt(arg);		
					} else if (arg.startsWith("n=")){
						arg = arg.replace("n=", "");
						NToTest = new int[]{Integer.parseInt(arg)};
					} else if (arg.startsWith("numrepeats=")){
						arg = arg.replace("numrepeats=", "");
						numRepeats = Integer.parseInt(arg);
					} else if (arg.startsWith("debuggingon=")){
						arg = arg.replace("debuggingon=", "");
						debuggingOn = Boolean.parseBoolean(arg);
					} else if (arg.startsWith("onethreadonly=")){
						arg = arg.replace("onethreadonly=", "");
						oneThreadOnly = Boolean.parseBoolean(arg);			
					}
				}
			}

			// otherwise ask for details
			else{
				Scanner sc = new Scanner(System.in);

				// how many threads?
				System.out.print("Should the sort run with two threads, rather than just one? (y/n): ");
				String input = sc.nextLine().toLowerCase();
				if(input.startsWith("y")){
					oneThreadOnly = false;
				} else {
					oneThreadOnly = true;
				}

				// get array sizes
				System.out.print("Please enter array sizes to test separated by spaces (I reccommend only one value under 800 for two threaded sort due to bugs): ");
				input = sc.nextLine().trim();
				String[] arraySizeString = input.split(" ");
				arraySize = new int[arraySizeString.length];
				for(int i = 0; i < arraySizeString.length; i++){
					arraySize[i] = Integer.parseInt(arraySizeString[i]);
				}

				// get NToTest
				System.out.print("Please enter values of N to test separated by spaces (I reccommend only one value under 6 for two threaded sort due to bugs): ");
				input = sc.nextLine().trim();
				String[] NToTestString = input.split(" ");
				NToTest = new int[NToTestString.length];
				for(int i = 0; i < NToTestString.length; i++){
					NToTest[i] = Integer.parseInt(NToTestString[i]);
				}

				// get numRepeats
				System.out.print("Please enter the number of times to repeat the test (I reccommend '1' for two threaded sort due to bugs): ");
				input = sc.nextLine();
				numRepeats = Integer.parseInt(input);

				// debugging?
				System.out.print("Debugging on? (y/n): ");
				input = sc.nextLine().toLowerCase();
				if(input.startsWith("y")){
					debuggingOn = true;
				}				
			}

			// run tests
			for(int size: arraySize){
				runTest(size, NToTest, numRepeats, debuggingOn, oneThreadOnly);	
			}

		} catch (Exception e){
			System.out.println(e.getMessage());
			System.out.println("Error parsing arguments. Exiting.");
			System.exit(0);
		}
	}
}
