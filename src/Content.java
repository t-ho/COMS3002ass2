import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
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
public class Content {

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
	private int contentPort = 23000; // default
	private int nameServerPort = 21000; // default
	private Selector selector = null;
	private DatagramChannel datagramChannel = null;
	private DatagramSocket datagramSocket = null;
	private String contentFileName;
	private List<ContentItem> items = new ArrayList<Content.ContentItem>();

	public Content(String[] args) {

		validateArguments(args);
		items = buildItemList();
		serverInit();
		handleRequests();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new Content(args);
	}

	/** Validate arguments **/
	private void validateArguments(String[] args) {
		if(args.length != 3) {
			System.err.print("Invalid command line arguments for Content\n");
			System.exit(1);
		}

		try {
			contentPort = Integer.parseInt(args[0]);
			contentFileName = args[1];
			nameServerPort = Integer.parseInt(args[2]);
			if (!validPort(contentPort) || !validPort(nameServerPort)){
				System.err.print("Invalid command line arguments for Content\n");
				System.exit(1);
			}
		}
		catch (NumberFormatException e){
			System.err.println("Invalid command line arguments for Content\n");
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
			datagramSocket.bind(new InetSocketAddress(contentPort));
		} catch (IOException e) {
			System.err.print("Content unable to listen on given port\n");
			System.exit(1);
		}
		try {
			ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
			// registers this channel with the given selector, returning a selection key
			datagramChannel.register(selector, SelectionKey.OP_READ, buffer);
			// register Content server with NameServer
			register();
			System.err.print("Content waiting for incoming connections\n");
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
						readBuffer.clear();

						int result = FAIL;
						// content request
						if(typeCommand == CONTENT_REQUEST) {
							/* Message format: itemID*/
							long itemID = Long.parseLong(message);
							message = "";
							for(int i = 0; i < items.size(); i++) {
								if(items.get(i).getID() == itemID) {
									/* Form message:
									 * message format: itemID + "\n" + content*/
									message = itemID + "\n" + items.get(i).getContent().toString();
									result = SUCCESS;
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

	/** Register Content server with NameServer **/
	private void register() {
		try {
			// construct datagram socket
			DatagramSocket clientSocket = new DatagramSocket();
			// set server's ip address
			InetAddress IPAddress = InetAddress.getByName(NAMESERVER_IP);
			// set buffers
			ByteBuffer sendBuffer = ByteBuffer.allocate(BUFFER_SIZE);
			ByteBuffer receiveBuffer = ByteBuffer.allocate(BUFFER_SIZE);
			byte[] sendData = new byte[BUFFER_SIZE];
			/* Message format: serverName + "\n" + serverIP + "\n" + serverPort */
			String command = "Content" + "\n" + InetAddress.getLocalHost().getHostAddress() + "\n" + contentPort + "\n";
			sendBuffer.putInt(REGISTER);
			sendBuffer.put(Charset.forName("UTF-8").encode(command));
			sendBuffer.flip();
			sendData = sendBuffer.array();
			// send the message to server
			DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, nameServerPort);
			DatagramPacket receivePacket = implementReliability(clientSocket, sendPacket);
			receiveBuffer = ByteBuffer.wrap(receivePacket.getData());
			int result = receiveBuffer.getInt();
			String message = Charset.forName("UTF-8").decode(receiveBuffer).toString();
			if(result == SUCCESS) {
				System.out.println(message);
			} else if(result == FAIL) {
				System.err.print("Content registration with NameServer failed\n");
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
		if(random >= 0.5) {
			ds.send(dp);
		}
		System.out.println(message);
	}

	/** Implements communication reliability. The sender process set a timeout for an ACK
	 * arrival and retransmit the message if the timeout expires.
	 * @throws IOException   
	 * @return return packet received from server */
	private DatagramPacket implementReliability(DatagramSocket clientSocket, 
			DatagramPacket sendPacket) throws IOException {
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
		simulatePacketLoss(clientSocket, sendPacket, "Message is sent to NameServer.");
		startTime = System.currentTimeMillis();
		thread.start();
		while(thread.isAlive()) {
			if(System.currentTimeMillis() - startTime > TIMEOUT && thread.isAlive()) {
				System.out.println("Timeout expired");
				// send and simulate packet loss
				simulatePacketLoss(clientSocket, sendPacket, "Message is retransmited to NameServer");
				startTime = System.currentTimeMillis();
			}
		}
		return receivePacket;
	}
	/** Read stock-file file into an internal data structure (Arraylist) **/
	private List<ContentItem> buildItemList() {
		List<ContentItem> itemList = new ArrayList<ContentItem>();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(contentFileName));
			String line = null;
			while ((line = reader.readLine()) != null) {
				String[] columns = line.split(" ");
				long id = Long.parseLong(columns[0]);
				String content = columns[1];
				itemList.add(new ContentItem(id, content));
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return itemList;
	}

	public class ContentItem {
		private long ID;
		private String content;

		public ContentItem(long ID, String content) {
			this.ID = ID;
			this.content = content;
		}

		public long getID() {
			return ID;
		}
		public void setID(long iD) {
			ID = iD;
		}
		public String getContent() {
			return content;
		}
		public void setContent(String content) {
			this.content = content;
		}

	}
}
