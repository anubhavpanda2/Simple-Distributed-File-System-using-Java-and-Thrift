import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import java.io.*;
import java.util.*;


/*
  Server class
  2. Creates the main thread for Server and WorkerNode interaction
*/

public class coordinatorNodeServer extends Thread {
    public static coordinatorNodeServerHandler handler;
    public static coordinatorNode.Processor processor;
    static String ip, port,Nr,Nw,N,synchTime,coordinatorPort;

    public static void readConfig(){
      	    try{
            	   //Read Config File for coordinatot ip and port
	           FileReader config = new FileReader("./data/config");
	    	   Properties configProperties = new Properties();
            	   configProperties.load(config);
	    	   Nr = configProperties.getProperty("Nr");
            	   Nw = configProperties.getProperty("Nw");
            	   N = configProperties.getProperty("N");
            	   synchTime = configProperties.getProperty("synchTime");
            	   coordinatorPort = configProperties.getProperty("coordinatorPort");
     	   }catch(Exception ex){
            	   ex.printStackTrace();
     	   }
    }

    public void run(){
		try{
			while (true){
				handler.synch();
				System.out.println("Synch operation done!");
				Thread.sleep(Long.parseLong(synchTime));
			}
		  }
    catch (Exception e){
      e.printStackTrace();
    }
	}

    public static void main(String [] args) {

        try {
            readConfig();
            handler = new coordinatorNodeServerHandler();
	    handler.setNr(Nr);
            handler.setNw(Nw);
            handler.setN(N);
            processor = new coordinatorNode.Processor(handler);

            new coordinatorNodeServer().start();

            Runnable simple = new Runnable() {
                public void run() {
                    simple(processor);
                }
            };
            new Thread(simple).start();
        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    public static void simple(coordinatorNode.Processor processor) {
        try {

                TServerTransport serverTransport = new TServerSocket(Integer.parseInt(coordinatorPort));
                TTransportFactory factory = new TFramedTransport.Factory();

                TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor).transportFactory(factory));
                server.serve();
        } catch (Exception e) {
            e.printStackTrace();

        }
    }
}
