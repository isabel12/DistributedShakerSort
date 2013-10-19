import java.net.InetAddress;
import java.net.UnknownHostException;


public class Main {

	private static final int masterPort = 4444;
	private static int[] toSort = {1,4,2,5,3};
	private static int N = 2;
	
	private static Node[] nodes = new Node[N];
	
	
	public static void start(){
		// get master hostname
		String masterHostname = "";
		try {
			masterHostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		// start the master
		Master master = new Master(N, masterPort, toSort);
		master.start();
		
		// create and start the nodes
		int port = masterPort;
		for(int i = 0; i < N; i++){
			nodes[i] = new Node(++port, masterHostname, masterPort);
			nodes[i].start();
		}
	}
	
	
	public static void main(String[] args){
		// extract out args
		
		// start
		Main.start();
	}
}
