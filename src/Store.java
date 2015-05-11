import java.io.BufferedReader;
import java.io.FileReader;
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
 * @author Minh Toan HO - 43129560
 *
 */
public class Store {

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

	int storePort = 24000; // default value
	int nameServerPort = 21000; // default value
	private Selector selector = null;
	private DatagramChannel datagramChannel = null;
	private DatagramSocket datagramSocket = null;
	String stockFileName;
	int bankPort;
	int contentPort;
	String bankIP;
	String contentIP;
	List<Item> items = new ArrayList<Item>();

	DatagramSocket bankSocket;
	InetAddress bankIPAddress;

	DatagramSocket contentSocket;
	InetAddress contentIPAddress;

	int result;
	String message;

	/**
	 * @param args
	 */
	public Store(String[] args) {

		validateArguments(args);
		/* register Store server to NameServer
		 * then get Bank and Content servers' info */
		register();
		items = buildItemList();
		try {
			bankSocket = new DatagramSocket();
			bankIPAddress = InetAddress.getByName(bankIP);
			contentSocket = new DatagramSocket();
			contentIPAddress = InetAddress.getByName(contentIP);
		} catch (IOException e) {
			e.printStackTrace();
		}
		serverInit();
		handleRequests();
	}

	/** Send a request to Bank server to check whether transaction is OK or NOT.
	 * The results are stored in global variable "result" and "message"**/
	private void getBankApproval(Item item, String creditCardNumber) {
		try {
			// set buffer
			ByteBuffer receiveBuffer = ByteBuffer.allocate(BUFFER_SIZE);
			/* command format: itemID + "\n" + itemPrice + "\n" + creditCardNumber */
			String command = item.getID() + "\n" + item.getPrice() + "\n" + creditCardNumber + "\n";
			DatagramPacket sendPacket = createSendPacket(bankIPAddress, bankPort, VALIDATE_TRANSACTION, command);
			DatagramPacket receivePacket = implementReliability(bankSocket, sendPacket, 
					"Validation request is sent to Bank server.");
			receiveBuffer = ByteBuffer.wrap(receivePacket.getData());
			result = receiveBuffer.getInt();
			message = Charset.forName("UTF-8").decode(receiveBuffer).toString();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/** Send a request to Content server to get content.
	 * The results are stored in global variable "result" and "message"**/
	private void getContent(Item item) {
		try {
			// set buffer
			ByteBuffer receiveBuffer = ByteBuffer.allocate(BUFFER_SIZE);
			/* command format: itemID */
			String command = item.getID() + "\n";
			DatagramPacket sendPacket = createSendPacket(contentIPAddress, contentPort, CONTENT_REQUEST, command);
			DatagramPacket receivePacket = implementReliability(contentSocket, sendPacket, 
					"Content request is sent to Content server.");
			receiveBuffer = ByteBuffer.wrap(receivePacket.getData());
			result = receiveBuffer.getInt();
			message = Charset.forName("UTF-8").decode(receiveBuffer).toString();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new Store(args);

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
			datagramSocket.bind(new InetSocketAddress(storePort));
		} catch (IOException e) {
			System.err.print("Store unable to listen on given port\n");
			System.exit(1);
		}
		try {
			ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
			// registers this channel with the given selector, returning a selection key
			datagramChannel.register(selector, SelectionKey.OP_READ, buffer);
			System.err.print("Store waiting for incoming connections\n");
		} catch (ClosedChannelException e) {
			e.printStackTrace();
		}
	}

	/** Read stock-file file into an internal data structure (Arraylist) **/
	private List<Item> buildItemList() {
		List<Item> itemList = new ArrayList<Store.Item>();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(stockFileName));
			String line = null;
			while ((line = reader.readLine()) != null) {
				String[] columns = line.split(" ");
				long id = Long.parseLong(columns[0]);
				float price = Float.parseFloat(columns[1]);
				itemList.add(new Item(id, price));
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return itemList;
	}

	/** Validate arguments **/
	private void validateArguments(String[] args){
		if(args.length != 3) {
			System.err.print("Invalid command line arguments for Store\n");
			System.exit(1);
		}

		try {
			storePort = Integer.parseInt(args[0]);
			stockFileName = args[1];
			nameServerPort = Integer.parseInt(args[2]);
			if (!validPort(nameServerPort) || !validPort(storePort)){
				System.err.print("Invalid command line arguments for Store\n");
				System.exit(1);
			}
			validPort(nameServerPort);
		}
		catch (NumberFormatException e){
			System.err.print("Invalid command line arguments for Store\n");
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
						message = Charset.forName("UTF-8").decode(readBuffer).toString();
						readBuffer.clear();

							// request for getting list of items
							if(typeCommand == LIST_ITEMS_REQUEST) {
								result = LIST_ITEMS_REQUEST;
								message = "";
								for(int i = 0; i < items.size(); i++) {
									message = message + (i + 1) + ". " + items.get(i).getID() + " " +
											items.get(i).getPrice() + "\n";
								}

							} else if(typeCommand == BUY_REQUEST ) { // buy request
								/*message format: orderNumber + "\n" + creditCardNumber*/
								String[] orderInfo = message.split("\n");
								int orderNumber = Integer.parseInt(orderInfo[0]);
								String creditCardNumber = orderInfo[1];
								int index = orderNumber - 1;
								/* The results are stored in the global variables "result" and "message" */
								getBankApproval(items.get(index), creditCardNumber);
								if(result == OK) {
									getContent(items.get(index));
									/* message get from Content server: itemID + "\n" + content */
									/* add itemPrice to message */
									message = message + "\n" + items.get(index).getPrice() + "\n";
									result = SUCCESS;
								} else if(result == NOT_OK) {
									message = items.get(index).getID() + "\n" + "transaction aborted";
									result = FAIL;
								}
							}
							readBuffer.putInt(result);
							readBuffer.put(Charset.forName("UTF-8").encode(message));
							readBuffer.flip();
							List<Object> objList = new ArrayList<Object>();
							objList.add(sa);
							objList.add(readBuffer);
							// set register status to WRITE
							dc.register(key.selector(), SelectionKey.OP_WRITE, objList);
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
			bankSocket.close();
			contentSocket.close();
		}
	}

	/** Register Store server with NameServer,
	 * Get Bank server's info,
	 * Get Content server's info
	 * **/
	private void register() {
		try {
			// construct datagram socket
			DatagramSocket clientSocket = new DatagramSocket();
			// set server's ip address
			InetAddress IPAddress = InetAddress.getByName(NAMESERVER_IP);
			// set buffers
			ByteBuffer receiveBuffer = ByteBuffer.allocate(BUFFER_SIZE);
			/* --------------Register with NameServer-------------- */
			/* Message format: serverName + "\n" + serverIP + "\n" + serverPort */
			String command = "Store" + "\n" + InetAddress.getLocalHost().getHostAddress() + "\n" + storePort + "\n";
			DatagramPacket sendPacket = createSendPacket(IPAddress, nameServerPort, REGISTER, command);
			DatagramPacket receivePacket = implementReliability(clientSocket, sendPacket, 
					"Registration's info is sent to NameServer.");
			receiveBuffer = ByteBuffer.wrap(receivePacket.getData());
			int result = receiveBuffer.getInt();
			String message = Charset.forName("UTF-8").decode(receiveBuffer).toString();
			if(result == SUCCESS) {
				System.out.println(message);
			} else if(result == FAIL) {
				System.err.print("Registration with NameServer failed\n");
				System.exit(1);
			}

			/* --------------Get Bank server's info-------------- */
			command = "Bank" + "\n";
			sendPacket = createSendPacket(IPAddress, nameServerPort, LOOKUP, command);
			receivePacket = implementReliability(clientSocket, sendPacket, 
					"Bank server's info request is sent to NameServer.");
			receiveBuffer = ByteBuffer.wrap(receivePacket.getData());
			result = receiveBuffer.getInt();
			message = Charset.forName("UTF-8").decode(receiveBuffer).toString();
			if(result == SUCCESS) {
				/* Message format: serverName + "\n" + serverIP + "\n" + serverPort */
				String[] serverInfo = message.split("\n");
				bankIP = serverInfo[1];
				bankPort = Integer.parseInt(serverInfo[2]);
			} else if(result == FAIL) {
				System.err.print("Bank has not registered\n");
				System.exit(1);
			}

			/* --------------Get Content server's info-------------- */
			command = "Content" + "\n";
			sendPacket = createSendPacket(IPAddress, nameServerPort, LOOKUP, command);
			receivePacket = implementReliability(clientSocket, sendPacket, 
					"Content server's info request is sent to NameServer.");
			receiveBuffer = ByteBuffer.wrap(receivePacket.getData());
			result = receiveBuffer.getInt();
			message = Charset.forName("UTF-8").decode(receiveBuffer).toString();
			if(result == SUCCESS) {
				/* Message format: serverName + "\n" + serverIP + "\n" + serverPort */
				String[] serverInfo = message.split("\n");
				contentIP = serverInfo[1];
				contentPort = Integer.parseInt(serverInfo[2]);
			} else if(result == FAIL) {
				System.err.print("Content has not registered\n");
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
		simulatePacketLoss(clientSocket, sendPacket, message);
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


	public class Item {
		private long ID;
		private float price;

		public Item(long ID, float price) {
			this.ID = ID;
			this.price = price;
		}

		public long getID() {
			return ID;
		}
		public void setID(long iD) {
			ID = iD;
		}
		public float getPrice() {
			return price;
		}
		public void setPrice(float price) {
			this.price = price;
		}

	}
}
