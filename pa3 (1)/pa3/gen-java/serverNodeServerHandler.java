import org.apache.thrift.TException;
import java.util.*;
import java.io.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
public class serverNodeServerHandler implements serverNode.Iface
{
  String coordinatorIP = "";
  String coordinatorPort = "";
  String selfIP = "";
  String selfPort = "";
  String filePath = "";
  String extension = "";

  Map<String,Integer> versionMap = new ConcurrentHashMap<>();
  //Used for ping
  @Override public boolean ping() throws TException {
	System.out.println("I got ping() normalServer");
	return true;
  }

  //Read from Config file
  public void readConfig(){
      try{
            //Read Config File for coordinatot ip and port
	    FileReader config = new FileReader("./data/config");
	    Properties configProperties = new Properties();
            configProperties.load(config);
	    coordinatorIP = configProperties.getProperty("coordinatorIP");
            coordinatorPort = configProperties.getProperty("coordinatorPort");
            selfPort = configProperties.getProperty("ServerNodePort");
            filePath = configProperties.getProperty("inputDir");
            extension = configProperties.getProperty("extension");
     }catch(Exception ex){
            ex.printStackTrace();
     }
  }

  //Node joining
  public void join() throws TException {
      readConfig();
      InetAddress ip;
      try {
         ip = InetAddress.getLocalHost();
         selfIP = ip.getHostName();
      } catch (UnknownHostException e) {
         System.out.println("Hostname not found");
      }
      TTransport  transport = new TSocket(coordinatorIP, Integer.parseInt(coordinatorPort));
      TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
      coordinatorNode.Client client = new coordinatorNode.Client(protocol);
      transport.open();
      client.join(selfIP,selfPort);
      transport.close();
  }

  //Used for Writing/Updating the files
  @Override public String write(String filename,String contents) throws TException
  {
     try{
      //System.out.println("naruto write");
      TTransport  transport = new TSocket(coordinatorIP, Integer.parseInt(coordinatorPort));
      TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
      coordinatorNode.Client client = new coordinatorNode.Client(protocol);
      transport.open();
      String quorum = client.assembleQuorom(selfIP,selfPort,1,filename);
      transport.close();
      //System.out.println("naruto write after assemble quorum");
      if(quorum.equals("System not ready"))
	 return "System not ready !";

      int idx = 0;
      String[] parts = quorum.split("#");
      String[] addressList = parts[0].split(",");
      String id = parts[1];
      int[] versionList = new int[addressList.length];
      //System.out.println("Inside Write : "+id);
      for(String address : addressList){
	 //System.out.println("inside quorum loop");
	 String[] addressParts = address.split(":");
         if(addressParts[0].equals(selfIP)&&addressParts[1].equals(selfPort)){
              versionList[idx++] = getversion(filename);
              continue;
         }
         TTransport  quorumTransport = new TSocket(addressParts[0], Integer.parseInt(addressParts[1]));
      	 TProtocol quorumProtocol = new TBinaryProtocol(new TFramedTransport(quorumTransport));
      	 serverNode.Client quorumClient = new serverNode.Client(quorumProtocol);
      	 quorumTransport.open();
      	 versionList[idx++] = quorumClient.getversion(filename);
      	 quorumTransport.close();
      }

      int maxIdx=0,maxVersion=versionList[0];
      for(int i=1;i<versionList.length;i++){
         if(versionList[i] > maxVersion){
             maxVersion = versionList[i];
             maxIdx = i;
         }
      }

     //Servers to be contacted for write operation
     for(String address : addressList){
	 String[] addressParts = address.split(":");
         if(addressParts[0].equals(selfIP)&&addressParts[1].equals(selfPort)){
              writeAll(filename,contents+"\nVersion : " + (maxVersion+1));
              updateVersion(filename,maxVersion+1);
              continue;
         }
         TTransport contactServerTransport = new TSocket(addressParts[0], Integer.parseInt(addressParts[1]));
      	 TProtocol contactServerProtocol = new TBinaryProtocol(new TFramedTransport(contactServerTransport));
      	 serverNode.Client contactServerClient = new serverNode.Client(contactServerProtocol);
      	 contactServerTransport.open();
      	 contactServerClient.writeAll(filename,contents+"\nVersion : " + (maxVersion+1));
         contactServerClient.updateVersion(filename,maxVersion+1);
      	 contactServerTransport.close();
      }

     transport.open();
     client.finished(selfIP,selfPort,1,Integer.parseInt(id),filename);
     //System.out.println("Id received : "+id);
     System.out.println("Write-- "+filename+extension+" : "+versionMap.get(filename));
     transport.close();
   }catch(Exception ex){
     ex.printStackTrace();
   }
     return filename + "successfully written";
  }

  @Override public Map<String, Integer> getMap()
  {
    return versionMap;
  }

  //used to write/update files into system
  @Override public String writeAll(String filename,String contents)
  {
     //Read folder path from config
     String path = filePath+filename+extension;
     File file = new File(path);
     try{
     	FileWriter writer = new FileWriter(file);
     	writer.write(contents);
     	writer.close();
     }catch(Exception ex){
        //System.out.println("Exception in writing file "+ex.getMessage());
	ex.printStackTrace();
     }
     return "done";
  }

  //Used for Reading the file
  @Override public String read(String filename) throws TException
  {
     try{
      //System.out.println("naruto");
      TTransport  transport = new TSocket(coordinatorIP, Integer.parseInt(coordinatorPort));
      TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
      coordinatorNode.Client client = new coordinatorNode.Client(protocol);
      transport.open();
      String quorum = client.assembleQuorom(selfIP,selfPort,0,filename);
      transport.close();
      //System.out.println("naruto after assemble quorum");
      if(quorum.equals("System not ready"))
	 return "System not ready !";

      int idx = 0;
      String[] parts = quorum.split("#");
      String[] addressList = parts[0].split(",");
      String id = parts[1];
      //System.out.println("Inside Read : "+id);
      int[] versionList = new int[addressList.length];
      for(String address : addressList){
	 String[] addressParts = address.split(":");
         if(addressParts[0].equals(selfIP)&&addressParts[1].equals(selfPort)){
              versionList[idx++] = getversion(filename);
              continue;
         }
         TTransport  quorumTransport = new TSocket(addressParts[0], Integer.parseInt(addressParts[1]));
      	 TProtocol quorumProtocol = new TBinaryProtocol(new TFramedTransport(quorumTransport));
      	 serverNode.Client quorumClient = new serverNode.Client(quorumProtocol);
      	 quorumTransport.open();
      	 versionList[idx++] = quorumClient.getversion(filename);
      	 quorumTransport.close();
      }

      int maxIdx=0,maxVersion=versionList[0];
      for(int i=1;i<versionList.length;i++){
         if(versionList[i] > maxVersion){
             maxVersion = versionList[i];
             maxIdx = i;
         }
      }

      //version 0 = file not found
      if(versionList[maxIdx]==0){
          transport.open();
          client.finished(selfIP,selfPort,0,Integer.parseInt(id),filename);
          transport.close();
          return "File Not Found";
      }

      //Server to be contacted for read operation
      String[] serverAddressParts = addressList[maxIdx].split(":");
      String fileContent = "";
      if(serverAddressParts[0].equals(selfIP)&&serverAddressParts[1].equals(selfPort)){
          fileContent = readAll(filename);
      }else{
	  TTransport contactServerTransport = new TSocket(serverAddressParts[0], Integer.parseInt(serverAddressParts[1]));
      	  TProtocol contactServerProtocol = new TBinaryProtocol(new TFramedTransport(contactServerTransport));
      	  serverNode.Client contactServerClient = new serverNode.Client(contactServerProtocol);
      	  contactServerTransport.open();
      	  fileContent = contactServerClient.readAll(filename);
      	  contactServerTransport.close();
      }

      transport.open();
      client.finished(selfIP,selfPort,0,Integer.parseInt(id),filename);
      transport.close();
      System.out.println("Read-- "+filename+extension+" : "+versionMap.get(filename));
      return fileContent;
    }catch(Exception ex){ex.printStackTrace();}
     return "Exception Exception Exception";
  }

  //used to read file from system
  @Override public String readAll(String filename) throws TException
  {
     //Read folder path from config
     String path = filePath+filename+extension;
     File file = new File(path);
     BufferedReader br;
     //System.out.println("Inside ReadAll");
     String line,fileContent="";
     try{
	br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
	line = null;
	while((line = br.readLine())!=null){
	    fileContent+=line+"\n";
	}
        return fileContent;
    }catch(Exception ex){
	System.out.println("File Not Found");
        return "File Not Found";
    }
  }

  //Used to get the latest version of the file
  @Override public int getversion(String filename)
  {
    if(versionMap.containsKey(filename))
       return versionMap.get(filename);
    return 0;
  }

  //Used to update the version after writing
  @Override public void updateVersion(String filename,int version)
  {
    versionMap.put(filename,version);
  }

}
