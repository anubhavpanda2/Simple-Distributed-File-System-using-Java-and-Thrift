import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import java.util.*;
import java.io.*;
public class Client {
    public static void main(String [] args) {
    /*  try {
           TTransport  transport = new TSocket("csel-kh4250-31.cselabs.umn.edu", 9096);
           TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
           serverNode.Client client = new serverNode.Client(protocol);
          // coordinatorNode.Client client = new coordinatorNode.Client(protocol);
           transport.open();
           System.out.println(client.getMap());
           transport .close();
       } catch(TException e) {

       }*/

       try{
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			System.out.println("Please enter the number of clients  ");
			int noOfClients = Integer.parseInt(br.readLine());

			ArrayList<String> IPList = new ArrayList<>();
			ArrayList<Integer> portsList = new ArrayList<>();
			ArrayList<String> fileList = new ArrayList<>();
      ArrayList<Integer> tasks = new ArrayList<>();
			ArrayList<Integer> requestsList = new ArrayList<>();

			for (int i=0;i<noOfClients;i++){
				System.out.println("Enter the IP and port of server for  client " + (i+1));
				//System.out.println("Please enter the IP and port on separate lines!");
				String IP = br.readLine();
				IPList.add(IP);
				int port = Integer.parseInt(br.readLine());
				portsList.add(port);

				System.out.println("Enter 0 for read and 1 for write");
				int task = Integer.parseInt(br.readLine());
				tasks.add(task);

				if (task == 0){
					System.out.println("Enter filename(operation : read )");
				}
				else if (task == 1){
					System.out.println("Enter filename (operation : write )  ");
				}

				String filename = br.readLine();
				fileList.add(filename);

				System.out.println("enter the number of  requests");
				int numberOfRequests = Integer.parseInt(br.readLine());
				requestsList.add(numberOfRequests);
			}

			SeparteClient[] clients = new SeparteClient[noOfClients];

			for (int i=0;i<noOfClients;i++){
				clients[i] = new SeparteClient(IPList.get(i), portsList.get(i), tasks.get(i), fileList.get(i), requestsList.get(i));
			}

			for (int i=0;i<noOfClients;i++){
				clients[i].start();
			}
		}
		catch (Exception e){
    e.printStackTrace();
  }
    }
}
