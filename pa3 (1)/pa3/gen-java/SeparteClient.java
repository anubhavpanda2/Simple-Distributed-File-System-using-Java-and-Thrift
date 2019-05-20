import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * Threaded version of the client which runs multiple requests in different threads *
 */

public class SeparteClient implements Runnable {
	private Thread t;

	//IP and Port of server
	private String IP;
	private int port;

	private int taskid; 				//Read or write task
	private String filename;		//Filename involved
	private int noOfRequests;	//Number of such requests the client wishes to issue

	//Initialize parameters
	public SeparteClient(String IP, int port, int taskid, String filename, int noOfRequests){
		this.IP = IP;
		this.port = port;
		this.taskid = taskid;
		this.filename = filename;
		this.noOfRequests = noOfRequests;
	}

	@Override
	/**
	 * run() method of thread
	 */
	public void run() {
		try{
			long startTime = System.currentTimeMillis();

			TTransport transport = new TSocket(IP, port);
			TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
			serverNode.Client client = new serverNode.Client(protocol);
			//System.out.println("baby"+" "+taskid+" "+noOfRequests);
			for (int i=0;i<noOfRequests;i++){
				transport.open();
			//	System.out.println("babyb"+" "+taskid+" "+noOfRequests);
				if (taskid == 1){
				//	contents = filename;
					String status = client.write(filename, filename);
					System.out.println("Operation no " + (i+1) + " : write " + status);
				}
				else if (taskid == 0){
					String contents = client.read(filename);
					System.out.println("Operation no " + (i+1) + " : read  contents  " + contents);
				}
			//	System.out.println(i+" "+taskid);
				transport.close();
			}
			startTime=System.currentTimeMillis() - startTime;
			String time=String.valueOf(startTime);
			System.out.println( "Total time taken for "+filename+" task "+taskid+" "+time);
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}

	public void start(){
		try{
			if (t == null){
				t = new Thread(this, "Thread"+IP+port);
				t.start();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

	}
}
