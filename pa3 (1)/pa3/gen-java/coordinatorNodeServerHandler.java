import org.apache.thrift.TException;
import java.util.*;
import java.io.*;
import java.util.concurrent.TimeUnit;
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
import java.util.concurrent.*;
import java.text.*;
import java.util.Date;
import java.util.Calendar;

public class coordinatorNodeServerHandler implements coordinatorNode.Iface
{
        int nr;
        int nw;
        int n;
        int noServersConnected = 0;
        String DetailsOfServersConnected="";
        int numOfRequest=0,processingReads=0;
        ConcurrentLinkedQueue<Integer> q = new ConcurrentLinkedQueue<Integer>();
        //new code
        ConcurrentHashMap<String,Integer>readRequests=new ConcurrentHashMap<>();//track the read requests of the file
        ConcurrentMap<String, ConcurrentLinkedQueue<Integer>> map = new ConcurrentHashMap<>();//maintains a queue per file
        //new code

        public void setNr(String val){
             nr = Integer.parseInt(val);
        }
	      public void setNw(String val){
             nw = Integer.parseInt(val);
        }
	       public void setN(String val){
             n = Integer.parseInt(val);
        }
        private final Object lock = new Object();  //lock for atomic operations on shared variables
        @Override public boolean ping() throws TException {
	         System.out.println("I got ping() coordinatorServer");
           return true;
        }
        //method is used to join a server node to the quorom
        @Override public void join(String ip,String port)
        {
          System.out.println("node joined "+ip+" "+port);
          DetailsOfServersConnected+=ip+":"+port+",";
          noServersConnected++;
        }
        // this method is used to get the quorum based on the read and write requests
        @Override public String assembleQuorom(String ip,String port,int task,String FileName)
        {
          try{
            if(noServersConnected<n)
            {
              System.out.println("not enough node has joined");
              return "System not ready";
            }

            int requestID=0;

            synchronized(lock){
              numOfRequest++;
              requestID=numOfRequest;
              //  q.add(requestID);
              if(task==1)
              {
                if(!map.containsKey(FileName))
                {
                  map.put(FileName,new ConcurrentLinkedQueue<Integer>());
                }
                ConcurrentLinkedQueue<Integer> queue=map.get(FileName);
                queue.add(requestID);
                map.put(FileName,queue);
              }

                //map.put(FileName,requestID);
            }

            //    System.out.println("before Assemble quoram");
            //while (q.peek()!= requestID&&task==1){
            while (task==1&&!map.get(FileName).isEmpty()&&map.get(FileName).peek()!= requestID){
              //System.out.println("Assemble quoram infinite"+requestID+" qid "+map.get(FileName).peek());
                 Thread.yield();
            }
            System.out.println(FileName+" "+numOfRequest+" "+requestID+" "+System.currentTimeMillis());//new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime()));
            if(task==0)
            {
                synchronized(lock){
                  //  processingReads++;
                    //new code
                    int val=0;
                    if(readRequests.containsKey(FileName))
                    {
                      val=readRequests.get(FileName);
                    }
                    val++;
                    readRequests.put(FileName,val);
                    //new code
                }
            }
            else
            {
              //newcode
                while(readRequests.containsKey(FileName)&&readRequests.get(FileName)!=0)
              //newcode
              //while(processingReads!=0)
              {
                Thread.yield();
              }
            }
            int returnsize=0;
            if(task==0)
              returnsize=nr;
            else
              returnsize=nw;
              //  System.out.println(returnsize + "#" + requestID);
            String[] serverlist = DetailsOfServersConnected.split(",");
            int noOfNodes = serverlist.length;
            int ctr=0;
            HashSet<Integer>usedIndexes=new HashSet<Integer>();
            String returnVal = "";
            while(ctr<returnsize)
            {
              int random=(int) (Math.random() * (noOfNodes));
              if (!usedIndexes.contains(random)){
                    usedIndexes.add(random);
                    //serverlist.append(nodeSplits[random] + ",");
                    returnVal+=serverlist[random]+",";
                    ctr++;
                     }

            }
            //System.out.println(returnVal + "#" + requestID);
            return returnVal + "#" + requestID;
          }
          catch(Exception e)
          {
            e.printStackTrace();
          }
          return "";
        }
        //This method is used to remove the read and write operations from the queue
        @Override public void finished(String ip,String port,int task,int id,String FileName)
        {
            if(task==0)
            {
              synchronized(lock){
                //System.out.println(id+"read "+FileName+" "+System.currentTimeMillis());
              //newcode
              int val=readRequests.get(FileName)-1;
              readRequests.put(FileName,val);
              System.out.println(FileName+" "+val);
              }

            //  newcode
              //  processingReads--;
            }
            else
            {
            //  System.out.println(id+" "+FileName+" "+System.currentTimeMillis());//new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime()));
            //  if (q.peek() == id){
				           // q.remove();
			        //       }
              synchronized(lock){
                     if(!map.get(FileName).isEmpty()&&map.get(FileName).peek() == id)
                     {
                       map.get(FileName).remove();
                     }
			                  else{
				                     System.out.println("Error!");
			                      }
                          }
            }
        }
        //method that is called in the background to synch the files
        @Override public void synch()
        {
        //  if(q.size()>0)
          //return;
          if(noServersConnected<n)
          {
            System.out.println("not enough node has joined");
            return;
          }
		       List<Map<String, Integer>> Maps = new ArrayList<>();
		        String[] serverlist = DetailsOfServersConnected.split(",");
		           for (String server: serverlist){
                 try {
			              String[] splits = server.split(":");
			              TTransport Transport = new TSocket(splits[0], Integer.parseInt(splits[1]));
			              TProtocol Protocol = new TBinaryProtocol(new TFramedTransport(Transport));
			              serverNode.Client Client = new serverNode.Client(Protocol);
			              Transport.open();
			              Maps.add(Client.getMap());//get the map of the other clients
			              Transport.close();
                  }
                  catch(Exception e)
                  {
                    e.printStackTrace();
                  }
		                                  }
		         List<String> uniqueFiles = getFileNames(Maps);

             for(String file:uniqueFiles)
             {
               List<Integer> fileVersions = new ArrayList<>();
               int maxVersion=-1;
               if(map.get(file).size()>0)
               continue;
               //find all file fileVersions
               for (Map<String, Integer> map: Maps){
				             if (map.containsKey(file)){
                            maxVersion=Math.max(maxVersion,map.get(file));
					                  fileVersions.add(map.get(file));
				              }
				             else{
					                  fileVersions.add(0);
				              }
			          }
                for(int i=0;i<fileVersions.size();i++)
                {
                  int version=fileVersions.get(i);
                  if(version<maxVersion)
                  {
                    System.out.println(maxVersion+" "+version+" "+"Synch " + file);
                    try{
                      String server=serverlist[i];
                      String[] splits=server.split(":");
                      TTransport Transport = new TSocket(splits[0], Integer.parseInt(splits[1]));
  			              TProtocol Protocol = new TBinaryProtocol(new TFramedTransport(Transport));
  			              serverNode.Client Client = new serverNode.Client(Protocol);
  			              Transport.open();
                      Client.writeAll(file, file + "\nVersion: " + (maxVersion));
  			              Client.updateVersion(file, maxVersion);
  			              Transport.close();
                    }
                    catch(Exception e)
                    {
                      e.printStackTrace();
                    }
                  }
                }
             }
        }

		//Helper method to find the unique files in the entire system
		private List<String> getFileNames(List<Map<String, Integer>> Maps) {
			List<String> res = new ArrayList<>();
			HashSet<String>data=new HashSet<String>();
			for (Map<String, Integer> map: Maps){
				for (String filename: map.keySet()){
					if (!data.contains(filename)){
						res.add(filename);
						data.add(filename);
					}
				}
			}
			return res;
		}
}
