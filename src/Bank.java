import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;


/**
 * 
 */

/**
 * @author Minh Toan HO - 43129560
 *
 */
public class Bank {

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

	// set Server parameters
	private int bankPort = 22000; // default
	private int nameServerPort = 21000; // default
	private Selector selector = null;
	private DatagramChannel datagramChannel = null;
	private DatagramSocket datagramSocket = null;

	public Bank(String[] args) {

		validateArguments(args);
		serverInit();
		handleRequests();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new Bank(args);
	}

	/** Validate arguments **/
	private void validateArguments(String[] args) {
		if(args.length != 2) {
			System.err.print("Invalid command line arguments for Bank\n");
			System.exit(1);
		}

		try {
			bankPort = Integer.parseInt(args[0]);
			nameServerPort = Integer.parseInt(args[1]);
			if (!validPort(bankPort) || !validPort(nameServerPort)){
				System.err.print("Invalid command line arguments for Bank\n");
				System.exit(1);
			}
		}
		catch (NumberFormatException e){
			System.err.println("Invalid command line arguments for Bank\n");
			System.exit(1);
		}
	}

	/**Check whether the port number in the range from 1024 to 65535**/
	private boolean validPort(int port) {
		if (port <= 1024 || port >= 65535){
			return false;
		} else {
			return true;
		}
	}

	/** Initialize server parameters **/
	private void serverInit() {
		try {
			// open selector
			selector = Selector.open();
			// open datagram channel
			datagramChannel = DatagramChannel.open();
			// set the socket associated with this channel
			datagramSocket = datagramChannel.socket();
			// set Blocking mode to non-blocking
			datagramChannel.configureBlocking(false);
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		try {
			// bind port
			datagramSocket.bind(new InetSocketAddress(bankPort));
		} catch (IOException e) {
			System.err.print("Bank unable to listen on given port\n");
			System.exit(1);
		}
		try {
			ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
			// registers this channel with the given selector, returning a selection key
			datagramChannel.register(selector, SelectionKey.OP_READ, buffer);
			// registers Bank server to NameServer
			register();
			System.err.print("Bank waiting for incoming connections\n");
		} catch (ClosedChannelException e) {
			e.printStackTrace();
		}
	}

	/** Handle requests, queries from Clients **/
	private void handleRequests() {
		try {
			while (selector.select() > 0) {
				for (SelectionKey key : selector.selectedKeys()) {
					// test whether this key's channel is ready for reading from Client
					if (key.isReadable()) {
						// get allocated buffer with size BUFFER_SIZE
						ByteBuffer readBuffer = (ByteBuffer) key.attachment();
						DatagramChannel dc = (DatagramChannel) key.channel();
						SocketAddress sa = dc.receive(readBuffer);
						readBuffer.flip();
						int typeCommand = readBuffer.getInt();
						String message = Charset.forName("UTF-8").decode(readBuffer).toString();
						readBuffer.flip();
						readBuffer.clear();

						int result = NOT_OK;
						// validate transaction request
						if(typeCommand == VALIDATE_TRANSACTION) {
							/* Message format: itemID + "\n" + itemPrice + "\n" + creditCardNumber */
							String[] serverInfo = message.split("\n");
							long itemID = Long.parseLong(serverInfo[0]);
							if(itemID % 2 == 1) { // itemID is odd
								result = OK;
								message = "";
								System.out.println(itemID + " " + "OK");
							} else { // itemID is even
								result = NOT_OK;
								message = "";
								System.out.println(itemID + " " + "NOT OK");
							}
							readBuffer.putInt(result);
							readBuffer.put(Charset.forName("UTF-8").encode(message));
							readBuffer.flip();
							List<Object> objList = new ArrayList<Object>();
							objList.add(sa);
							objList.add(readBuffer);
							// set register status to WRITE
							dc.register(key.selector(), SelectionKey.OP_WRITE, objList);
						} else {
							System.out.println("Invalid command");
						}
					}
					// test whether this key's channel is ready for sending to Client
					else if (key.isWritable()) {
						DatagramChannel dc = (DatagramChannel) key.channel();
						List<?> objList = (ArrayList<?>) key.attachment();
						SocketAddress sa = (SocketAddress) objList.get(0);
						ByteBuffer writeBuffer = (ByteBuffer) objList.get(1);
						dc.send(writeBuffer, sa);
						writeBuffer.clear();
						// set register status to READ
						dc.register(selector, SelectionKey.OP_READ, writeBuffer);
					}
				}
				if (selector.isOpen()) {
					selector.selectedKeys().clear();
				} else {
					break;
				}
			}
		} catch (ClosedChannelException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (datagramChannel != null) {
				try {
					datagramChannel.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/** Register Bank server with NameServer **/
	private void register() {
		try {
			// construct datagram socket
			DatagramSocket clientSocket = new DatagramSocket();
			// set server's ip address
			InetAddress IPAddress = InetAddress.getByName(NAMESERVER_IP);
			// set buffers
			ByteBuffer receiveBuffer = ByteBuffer.allocate(BUFFER_SIZE);
			/* Message format: serverName + "\n" + serverIP + "\n" + serverPort */
			String command = "Bank" + "\n" + InetAddress.getLocalHost().getHostAddress() + "\n" + bankPort + "\n";
			DatagramPacket sendPacket = createSendPacket(IPAddress, nameServerPort, REGISTER, command);
			DatagramPacket receivePacket = implementReliability(clientSocket, sendPacket, 
					"Registration's info is sent to NameServer");
			receiveBuffer = ByteBuffer.wrap(receivePacket.getData());
			int result = receiveBuffer.getInt();
			String message = Charset.forName("UTF-8").decode(receiveBuffer).toString();
			if(result == SUCCESS) {
				System.out.println(message);
			} else if(result == FAIL) {
				System.err.print("Bank registration to NameServer failed\n");
				System.exit(1);
			}
			// close up
			clientSocket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/** Simulate the packet loss 
	 * @throws IOException */
	private void simulatePacketLoss(DatagramSocket ds, DatagramPacket dp, String message) throws IOException {
		double random = Math.random();
		System.out.println(message);
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
		simulatePacketLoss(clientSocket, sendPacket, "  >>> " + message);
		startTime = System.currentTimeMillis();
		thread.start();
		while(thread.isAlive()) {
			if(System.currentTimeMillis() - startTime > TIMEOUT && thread.isAlive()) {
				System.out.println("Timeout expired");
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
}
