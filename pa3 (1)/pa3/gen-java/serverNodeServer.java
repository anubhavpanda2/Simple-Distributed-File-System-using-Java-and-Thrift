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

public class serverNodeServer {
    public static serverNodeServerHandler handler;
    public static serverNode.Processor processor;
    static String ServerNodePort;
    public static void readConfig(){
      	    try{
            	   //Read Config File for coordinatot ip and port
	           FileReader config = new FileReader("./data/config");
	    	   Properties configProperties = new Properties();
            	   configProperties.load(config);
            	   ServerNodePort = configProperties.getProperty("ServerNodePort");
     	   }catch(Exception ex){
            	   ex.printStackTrace();
     	   }
    }

    public static void main(String [] args) {

        try {
            readConfig();
            handler = new serverNodeServerHandler();
            processor = new serverNode.Processor(handler);
            handler.join();

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

    public static void simple(serverNode.Processor processor) {
        try {

                TServerTransport serverTransport = new TServerSocket(Integer.parseInt(ServerNodePort));
                TTransportFactory factory = new TFramedTransport.Factory();

                TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor).transportFactory(factory));
                server.serve();
        } catch (Exception e) {
            e.printStackTrace();

        }
    }
}
