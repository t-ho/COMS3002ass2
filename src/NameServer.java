import java.io.IOException;
import java.net.DatagramSocket;
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
public class NameServer {

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


	// set Server parameters
	private int port = 21000;
	private Selector selector = null;
	private DatagramChannel datagramChannel = null;
	private DatagramSocket datagramSocket = null;
	private List<Server> serverList = new ArrayList<Server>();  

	public NameServer(String[] args) {
		validateArguments(args);
		serverInit();
		handleRequests();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new NameServer(args);
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
			datagramSocket.bind(new InetSocketAddress(port));
		} catch (IOException e) {
			System.err.print("Cannot listen on given port number " + port + "\n");
			System.exit(1);
		}
		try {
			ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
			// registers this channel with the given selector, returning a selection key
			datagramChannel.register(selector, SelectionKey.OP_READ, buffer);
			System.err.print("Name Server waiting for incoming connections ...\n");
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

						// react by Client's message
						int result = FAIL;
						// register queries
						if(typeCommand == REGISTER) {
							/* Message format: serverName + "\n" + serverIP + "\n" + serverPort */
							String[] serverInfo = message.split("\n");
							String name = serverInfo[0];
							boolean isFound = false;
							for(int i = 0; i < serverList.size(); i++) {
								if(serverList.get(i).getServerName().equals(name)) {
									isFound = true;
									break;
								}
							}
							if(isFound == true) { // server's name has already registered
								result = FAIL;
								message = "FAIL";
								System.out.println("Registration failed: " + serverInfo[0] + " server had already registered.");
							} else { // isFound == false
								serverList.add(new Server(serverInfo[0], serverInfo[1], 
										Integer.parseInt(serverInfo[2])));
								result = SUCCESS;
								message = "Registration with NameServer succeeded.";
								System.out.println("Registered: " + serverInfo[0] + " " +
										serverInfo[1] + " " + serverInfo[2]);
							}
						} else if(typeCommand == LOOKUP ) { // lookup queries
							boolean isFound = false;
							String[] serverInfo = message.split("\n");
							String name = serverInfo[0];
							result = FAIL;
							for(int i = 0; i < serverList.size(); i++) {
								if(serverList.get(i).getServerName().equals(name)) {
									isFound = true;
									result = SUCCESS;
									message = serverList.get(i).getServerName() + "\n" +
											serverList.get(i).getIPAddress() + "\n" +
											serverList.get(i).getPort() + "\n";
									break;
								}
							}
							if(isFound == false) {
								result = FAIL;
								message = "Error: Process has not registerd with the Name Server\n";
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

	/** Validate arguments **/
	private void validateArguments(String[] args) {
		if(args.length != 1) {
			System.err.print("Invalid command line arguments for NameServer\n");
			System.exit(1);
		}

		try {
			port = Integer.parseInt(args[0]);
			if (!validPort(port)){
				System.err.print("Invalid command line arguments for NameServer\n");
				System.exit(1);
			}
		}
		catch (NumberFormatException e){
			System.err.println("Invalid command line arguments for NameServer\n");
			System.exit(1);
		}
	}

	/** Class to store information of server **/
	public class Server {
		private String serverName;
		private String iPAddress;
		private int port;

		public Server(String name, String iPAddress, int port) {
			this.serverName = name;
			this.iPAddress = iPAddress;
			this.port = port;
		}

		public String getServerName() {
			return serverName;
		}
		public void setServerName(String name) {
			this.serverName = name;
		}
		public String getIPAddress() {
			return iPAddress;
		}
		public void setIPAddress(String iPAddress) {
			this.iPAddress = iPAddress;
		}
		public int getPort() {
			return port;
		}
		public void setPort(int port) {
			this.port = port;
		}
	}
}
