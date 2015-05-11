import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;


/**
 * 
 */

/**
 * @author Minh Toan HO - 43129560
 *
 */
public class Client {

	public final int BUFFER_SIZE = 1024;
	public final int REGISTER = 1;
	public final int LOOKUP = 2;
	public final int SUCCESS = 3;
	public final int FAIL = 4;
	public final int GET_BANK_INFO = 5;
	public final int GET_CONTENT_INFO = 6;
	public final int OK = 7;
	public final int NOT_OK = 8;
	public final int VALIDATE_TRANSACTION = 9;
	public final int CONTENT_REQUEST = 10;
	public final int LIST_ITEMS_REQUEST = 11;
	public final int BUY_REQUEST = 12;

	public final String NAMESERVER_IP = "127.0.0.1";
	public final long TIMEOUT = 1000;

	private int requestNumber;
	private int nameServerPort = 21000; // default
	private String storeIP;
	private int storePort;
	private String creditCardNumber = "1234567891234567";


	public Client(String[] args) {

		validateArguments(args);
		getStoreServerInfo();
		connectToStore();
	}



	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new Client(args);
	}

	/** Connect to Store server **/
	private void connectToStore() {		
		try {
			// construct datagram socket
			DatagramSocket clientSocket = new DatagramSocket();
			// set server's ip address
			InetAddress IPAddress = InetAddress.getByName(storeIP);
			// set buffers
			ByteBuffer receiveBuffer = ByteBuffer.allocate(BUFFER_SIZE);
			String command = "";
			int result = 0;
			int typeCommand = 0;
			if(requestNumber == 0) { // 0 means that a list of items will be requested from Store
				typeCommand = LIST_ITEMS_REQUEST;
				command = "list of items";
			} else if(requestNumber >= 1 && requestNumber <= 10) { // 1 <= requestNumber <= 10 means that buy request
				typeCommand = BUY_REQUEST;
				command = requestNumber + "\n" + creditCardNumber + "\n";
			}
			DatagramPacket sendPacket = createSendPacket(IPAddress, storePort, typeCommand, command);
			DatagramPacket receivePacket = implementReliability(clientSocket, sendPacket, "Request is sent"); 
			receiveBuffer = ByteBuffer.wrap(receivePacket.getData());
			result = receiveBuffer.getInt();
			String message = Charset.forName("UTF-8").decode(receiveBuffer).toString();
			receiveBuffer.clear();

			if(typeCommand == LIST_ITEMS_REQUEST) {
				System.out.println(message);
			} else if(typeCommand == BUY_REQUEST) {
				if(result == SUCCESS) {
					/* Message format: itemID + "\n" + content + "\n" + itemPrice */
					String[] purchasedItem = message.split("\n");
					String id = purchasedItem[0];
					String content = purchasedItem[1];
					String price = purchasedItem[2];
					System.out.print(id + " ($ " + price + ") CONTENT " + content + "\n");
				} else if(result == FAIL) {
					/* Message format: itemID + "\n" + "transaction aborted" */
					String[] info = message.split("\n");
					System.out.print(info[0] + " " + info[1] + "\n");
				}
			}
			// close up
			clientSocket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/** Validate arguments **/
	private void validateArguments(String[] args) {
		if(args.length != 2) {
			System.err.print("Invalid command line arguments\n");
			System.exit(1);
		}

		try {
			requestNumber = Integer.parseInt(args[0]);
			nameServerPort = Integer.parseInt(args[1]);
			if (requestNumber < 0 || requestNumber > 10){
				System.err.print("Invalid command line arguments\n");
				System.exit(1);
			}
			if (!validPort(nameServerPort)){
				System.err.print("Invalid command line arguments\n");
				System.exit(1);
			}
		}
		catch (NumberFormatException e){
			System.err.println("Invalid command line arguments\n");
			System.exit(1);
		}
	}

	/** Gets Store server's info from NameServer.
	 * Store's IP is stored in global variable storeIP.
	 * Store's port is stored in global variable storePort. **/
	private void getStoreServerInfo() {		
		try {
			// construct datagram socket
			DatagramSocket clientSocket = new DatagramSocket();
			// set server's ip address
			InetAddress IPAddress = InetAddress.getByName(NAMESERVER_IP);
			// set buffers
			ByteBuffer receiveBuffer = ByteBuffer.allocate(BUFFER_SIZE);
			/* Message format: serverName + "\n" + serverIP + "\n" + serverPort */
			String command = "Store" + "\n";
			DatagramPacket sendPacket = createSendPacket(IPAddress, nameServerPort, LOOKUP, command);
			DatagramPacket receivePacket = implementReliability(clientSocket, sendPacket, 
					"Store server's info request is sent to NameServer.");
			receiveBuffer = ByteBuffer.wrap(receivePacket.getData());
			int result = receiveBuffer.getInt();
			String message = Charset.forName("UTF-8").decode(receiveBuffer).toString();
			if(result == SUCCESS) {
				/* Message format: serverName + "\n" + serverIP + "\n" + serverPort */
				String[] serverInfo = message.split("\n");
				storeIP = serverInfo[1];
				storePort = Integer.parseInt(serverInfo[2]);
			} else if(result == FAIL) {
				System.err.print("Store has not registered\n");
				System.exit(1);
			}
			// close up
			clientSocket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/** Simulate the packet loss 
	 * @throws IOException */
	private void simulatePacketLoss(DatagramSocket ds, DatagramPacket dp, String message) throws IOException {
		double random = Math.random();
		//System.out.println(message);
		if(random >= 0.5) {
			ds.send(dp);
		}
	}


	/** Implements communication reliability. The sender process set a timeout for an ACK
	 * arrival and retransmit the message if the timeout expires.
	 * @throws IOException   
	 * @return return packet received from server */
	private DatagramPacket implementReliability(DatagramSocket clientSocket, 
			DatagramPacket sendPacket, String message) throws IOException {
		long startTime = 0;
		byte[] receiveData = new byte[BUFFER_SIZE];
		DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
		Thread thread = new Thread( new Runnable() {
			@Override
			public void run() {
				try {
					// receive reply message from server
					clientSocket.receive(receivePacket);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});

		// send and simulate packet loss
		simulatePacketLoss(clientSocket, sendPacket, message);
		startTime = System.currentTimeMillis();
		thread.start();
		while(thread.isAlive()) {
			if(System.currentTimeMillis() - startTime > TIMEOUT && thread.isAlive()) {
				//System.out.println("Timeout expired");
				// send and simulate packet loss
				simulatePacketLoss(clientSocket, sendPacket, "RETRANSMIT: " + message);
				startTime = System.currentTimeMillis();
			}
		}
		return receivePacket;
	}

	/** Create a send packet */
	private DatagramPacket createSendPacket(InetAddress iPAddress, int port, int typeCommand, String command) {
		byte[] sendData = new byte[BUFFER_SIZE];
		ByteBuffer sendBuffer = ByteBuffer.allocate(BUFFER_SIZE);
		sendBuffer.putInt(typeCommand);
		sendBuffer.put(Charset.forName("UTF-8").encode(command));
		sendBuffer.flip();
		sendData = sendBuffer.array();
		// send the message to server
		DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, iPAddress, port);
		return sendPacket;
	}

	/**Check whether the port number in the range from 1024 to 65535**/
	private boolean validPort(int port) {
		if (port <= 1024 || port >= 65535){
			return false;
		} else {
			return true;
		}
	}
}
