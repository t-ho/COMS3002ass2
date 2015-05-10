import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
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
	private ServerSocketChannel serverSocketChannel = null;
	private ServerSocket serverSocket = null;
	String stockFileName;
	int bankPort;
	int contentPort;
	String bankIP;
	String contentIP;
	List<Item> items = new ArrayList<Item>();

	SocketChannel bankChannel;
	Selector bankSelector;

	SocketChannel contentChannel;
	Selector contentSelector;

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
		connectBankServer();
		connectContentServer();
		serverInit();
		handleRequests();
	}
	/** Connect to Bank server **/
	private void connectBankServer() {
		try {
			// open socket channel
			bankChannel = SocketChannel.open();
			// set Blocking mode to non-blocking
			bankChannel.configureBlocking(false);
			// set Server info
			InetSocketAddress target = new InetSocketAddress(bankIP, bankPort);
			// open selector
			bankSelector = Selector.open();
			// connect to Server
			bankChannel.connect(target);
			// registers this channel with the given selector, returning a selection key
			bankChannel.register(bankSelector, SelectionKey.OP_CONNECT);

			while (bankSelector.select() > 0) {
				for (SelectionKey bankKey : bankSelector.selectedKeys()) {
					// test connectivity
					if (bankKey.isConnectable()) {
						SocketChannel sc = (SocketChannel) bankKey.channel();
						// set register status to WRITE
						sc.register(bankSelector, SelectionKey.OP_WRITE);
						try {
							sc.finishConnect();
						} catch(IOException ie) {
							System.err.print("Unable to connect with Bank\n");
							System.exit(1);
						}
						System.out.println("Connect to Bank successfully");
						break;
					}
				}
				if (bankSelector.isOpen()) {
					bankSelector.selectedKeys().clear();
				}
				break;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/** Connect to Content server **/
	private void connectContentServer() {
		try {
			// open socket channel
			contentChannel = SocketChannel.open();
			// set Blocking mode to non-blocking
			contentChannel.configureBlocking(false);
			// set Server info
			InetSocketAddress target = new InetSocketAddress(contentIP, contentPort);
			// open selector
			contentSelector = Selector.open();
			// connect to Server
			contentChannel.connect(target);
			// registers this channel with the given selector, returning a selection key
			contentChannel.register(contentSelector, SelectionKey.OP_CONNECT);

			while (contentSelector.select() > 0) {
				for (SelectionKey contentKey : contentSelector.selectedKeys()) {
					// test connectivity
					if (contentKey.isConnectable()) {
						SocketChannel sc = (SocketChannel) contentKey.channel();
						// set register status to WRITE
						sc.register(contentSelector, SelectionKey.OP_WRITE);
						try {
							sc.finishConnect();
						} catch(IOException ie) {
							System.err.print("Unable to connect with Content\n");
							System.exit(1);
						}
						System.out.println("Connect to Content successfully");
						break;
					}
				}
				if (contentSelector.isOpen()) {
					contentSelector.selectedKeys().clear();
				}
				break;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/** Send a request to Bank server to check whether transaction is OK or NOT.
	 * The results are stored in global variable "result" and "message"**/
	private void getBankApproval(Item item, String creditCardNumber) {
		try {
			while (bankSelector.select() > 0) {
				boolean isDone = false;
				for (SelectionKey bankKey : bankSelector.selectedKeys()) {
					if(bankKey.isWritable()) {
						SocketChannel sc = (SocketChannel) bankKey.channel();
						String command = "";
						/* command format: itemID + "\n" + itemPrice + "\n" + creditCardNumber */
						command = item.getID() + "\n" + item.getPrice() + "\n" + creditCardNumber;
						writeCommand(VALIDATE_TRANSACTION, command, bankChannel);

						// set register status to READ
						sc.register(bankSelector, SelectionKey.OP_READ);
					}else if(bankKey.isReadable()) {
						// allocate a byte buffer with size 1024
						ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
						SocketChannel sc = (SocketChannel) bankKey.channel();
						int readBytes = 0;
						// try to read bytes from the channel into the buffer
						try {
							int ret = 0;
							try {
								while ((ret = sc.read(buffer)) > 0)
									readBytes += ret;
							} finally {
								buffer.flip();
							}
							// finished reading, print to Client
							if (readBytes > 0) {
								result = buffer.getInt();
								message = Charset.forName("UTF-8").decode(buffer).toString();
								buffer = null;
							}
						} finally {
							if (buffer != null)
								buffer.clear();
						}
						// set register status to WRITE
						sc.register(bankSelector, SelectionKey.OP_WRITE);
						isDone = true;
						break;
					}
				}
				if(isDone == true) {
					break;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/** Send a request to Content server to get content.
	 * The results are stored in global variable "result" and "message"**/
	private void getContent(Item item) {
		try {
			while (contentSelector.select() > 0) {
				boolean isDone = false;
				for (SelectionKey contentKey : contentSelector.selectedKeys()) {
					if(contentKey.isWritable()) {
						SocketChannel sc = (SocketChannel) contentKey.channel();
						String command = "";
						/* command format: itemID */
						command = item.getID() + "";
						writeCommand(CONTENT_REQUEST, command, contentChannel);

						// set register status to READ
						sc.register(contentSelector, SelectionKey.OP_READ);
					}else if(contentKey.isReadable()) {
						// allocate a byte buffer with size 1024
						ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
						SocketChannel sc = (SocketChannel) contentKey.channel();
						int readBytes = 0;
						// try to read bytes from the channel into the buffer
						try {
							int ret = 0;
							try {
								while ((ret = sc.read(buffer)) > 0)
									readBytes += ret;
							} finally {
								buffer.flip();
							}
							// finished reading
							if (readBytes > 0) {
								result = buffer.getInt();
								message = Charset.forName("UTF-8").decode(buffer).toString();
							}
						} finally {
							if (buffer != null)
								buffer.clear();
						}
						// set register status to WRITE
						sc.register(contentSelector, SelectionKey.OP_WRITE);
						isDone = true;
						break;
					}
				}
				if(isDone == true) {
					break;
				}
			}
		} catch (IOException e) {
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
			// open socket channel
			serverSocketChannel = ServerSocketChannel.open();
			// set the socket associated with this channel
			serverSocket = serverSocketChannel.socket();
			// set Blocking mode to non-blocking
			serverSocketChannel.configureBlocking(false);
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		try {
			// bind port
			serverSocket.bind(new InetSocketAddress(storePort));
		} catch (IOException e) {
			System.err.print("Store unable to listen on given port\n");
			System.exit(1);
		}
		try {
			// registers this channel with the given selector, returning a selection key
			serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
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

	/** Write command to channel **/
	private void writeCommand(int typeCommand, String command, SocketChannel channel) {
		ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
		buffer.putInt(typeCommand);
		buffer.put(Charset.forName("UTF-8").encode(command));
		buffer.flip();
		// send to Server
		try {
			channel.write(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		}
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
					// test whether this key's channel is ready to accept a new socket connection
					if (key.isAcceptable()) {
						// accept the connection
						ServerSocketChannel server = (ServerSocketChannel) key.channel();
						SocketChannel sc = server.accept();
						if (sc == null)
							continue;
						System.out.println("Connection accepted from: " + sc.getRemoteAddress());
						// set blocking mode of the channel
						sc.configureBlocking(false);
						// allocate buffer
						ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
						// set register status to READ
						sc.register(selector, SelectionKey.OP_READ, buffer);
					}
					// test whether this key's channel is ready for reading from Client
					else if (key.isReadable()) {
						// get allocated buffer with size BUFFER_SIZE
						ByteBuffer buffer = (ByteBuffer) key.attachment();
						SocketChannel sc = (SocketChannel) key.channel();
						int readBytes = 0;
						int typeCommand = 0;

						// try to read bytes from the channel into the buffer
						try {
							int ret;
							try {
								while ((ret = sc.read(buffer)) > 0)
									readBytes += ret;
							} catch (Exception e) {
								readBytes = 0;
							} finally {
								buffer.flip();
							}
							// finished reading, form message
							if (readBytes > 0) {
								typeCommand = buffer.getInt();
								message = Charset.forName("UTF-8").decode(buffer).toString();
							}
						} finally {
							if (buffer != null)
								buffer.clear();
						}

						// react by Client's message
						if (readBytes > 0) {
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
									message = message + "\n" + items.get(index).getPrice();
									result = SUCCESS;
								} else if(result == NOT_OK) {
									message = items.get(index).getID() + "\n" + "transaction aborted";
									result = FAIL;
								}
							}
							buffer.putInt(result);
							buffer.put(Charset.forName("UTF-8").encode(message));
							sc.register(key.selector(), SelectionKey.OP_WRITE, buffer);
						}
					}
					// test whether this key's channel is ready for sending to Client
					else if (key.isWritable()) {
						SocketChannel sc = (SocketChannel) key.channel();
						ByteBuffer buffer = (ByteBuffer) key.attachment();
						buffer.flip();
						sc.write(buffer);
						// set register status to READ
						sc.register(key.selector(), SelectionKey.OP_READ, buffer);
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
			if (serverSocketChannel != null) {
				try {
					serverSocketChannel.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
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
			DatagramPacket sendPacket = createSendPacket(IPAddress, REGISTER, command);
			DatagramPacket receivePacket = implementReliability(clientSocket, sendPacket, "Registration's info");
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
			sendPacket = createSendPacket(IPAddress, LOOKUP, command);
			receivePacket = implementReliability(clientSocket, sendPacket, "Bank server's info request");
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
			sendPacket = createSendPacket(IPAddress, LOOKUP, command);
			receivePacket = implementReliability(clientSocket, sendPacket, "Content server's info request");
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
		simulatePacketLoss(clientSocket, sendPacket, message + " is sent to NameServer.");
		startTime = System.currentTimeMillis();
		thread.start();
		while(thread.isAlive()) {
			if(System.currentTimeMillis() - startTime > TIMEOUT && thread.isAlive()) {
				System.out.println("Timeout expired");
				// send and simulate packet loss
				simulatePacketLoss(clientSocket, sendPacket, message + " is retransmited to NameServer");
				startTime = System.currentTimeMillis();
			}
		}
		return receivePacket;
	}

	/** Create a send packet */
	private DatagramPacket createSendPacket(InetAddress iPAddress, int typeCommand, String command) {
		byte[] sendData = new byte[BUFFER_SIZE];
		ByteBuffer sendBuffer = ByteBuffer.allocate(BUFFER_SIZE);
		sendBuffer.putInt(typeCommand);
		sendBuffer.put(Charset.forName("UTF-8").encode(command));
		sendBuffer.flip();
		sendData = sendBuffer.array();
		// send the message to server
		DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, iPAddress, nameServerPort);
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
