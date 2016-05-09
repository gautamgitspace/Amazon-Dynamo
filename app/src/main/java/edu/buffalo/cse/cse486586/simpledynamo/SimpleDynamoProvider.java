package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.provider.ContactsContract;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider
{

    ArrayList<Integer> nodeSpace = new ArrayList<Integer>(5);
    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    String lDump="@";
    String gDump="*";
    private int launchPort;
    static final int serverPort = 10000;
    int successorOnePort=0;
    int successorTwoPort=0;
    int predecessorOnePort=0;
    int predecessorTwoPort=0;
    public ReentrantLock mutex =  new ReentrantLock();
    Context contextType;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs)
    {
        // TODO Auto-generated method stub
        String key=selection;


        contextType=getContext();
        try
        {
            contextType.deleteFile(key);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        for(int i=0;i<nodeSpace.size();i++)
        {
            if(launchPort!=nodeSpace.get(i))
            {
                NodeTalk communication = new NodeTalk("distributedDelete");
                communication.setKey(key);
                try
                {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), nodeSpace.get(i) * 2);
                    ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                    Log.v(TAG, "about to send distributedDelete to stream");
                    outputStream.writeObject(communication);
                    Log.v(TAG, "object written with message: " + communication.getLanguage() +
                    "and source port " + communication.getWhoAmI() + "and destination port " + communication.getSuccessor1());

                    socket.close();
                }
                catch(Exception e)
                {
                    e.getMessage();
                }

            }
        }

        return 0;
    }

    @Override
    public String getType(Uri uri)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values)
    {
        //mutex.lock();
        String key = (String)values.get("key");
        String value = (String)values.get("value");

        int association=0;
        int s1IndexForAssociation=0;
        int s2IndexForAssociation=0;
        int s1PortForAssociation=0;
        int s2PortForAssociation=0;
        int associationIndex=0;

        Iterator iterator = nodeSpace.iterator();
        while(iterator.hasNext())
        {
            try
            {
                String string=(iterator.next()).toString();
                if(genHash(key).compareTo(genHash(string)) <=0)
                {
                    association=Integer.parseInt(string);
                    break;
                }
            }
            catch (NoSuchAlgorithmException e)
            {
                Log.v(TAG,"Exception in key comparison");
            }
        }
        if(association == 0)
        {
            association = nodeSpace.get(0);
        }

        Log.v(TAG, "Key: " + "[ " + key + " ]" + " associated with: " + association);

        for(int i=0; i<nodeSpace.size(); i++)
        {
            if(nodeSpace.get(i)==association)
            {
                associationIndex=i;
                break;
            }
        }

        s1IndexForAssociation=(associationIndex+1)%5;
        s2IndexForAssociation=(associationIndex+2)%5;
        s1PortForAssociation=nodeSpace.get(s1IndexForAssociation);
        s2PortForAssociation=nodeSpace.get(s2IndexForAssociation);


        if(association==launchPort || s1PortForAssociation==launchPort || s2PortForAssociation==launchPort)
        {
            //LOCAL INSERT HERE ONLY
            Log.v(TAG,"INSERT BLOCK 1");
            Log.v(TAG,"association = launch port case. Key: " + key + "inserted locally at " + association);
            try
            {
                FileOutputStream fos = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                fos.write(value.getBytes());
                fos.close();
            }catch(IOException ioe){
                Log.e("Insert", ioe.getMessage());
            }catch(NullPointerException npe){
                Log.e("Insert", npe.getMessage());
            }catch(Exception e){
                Log.e("Insert", e.getMessage());
            }

        }
        if(association!=launchPort)
        {
            Log.v(TAG,"INSERT BLOCK 2");
            //PREPARE OBJECT FOR REPLICATED INSERT and SEND TO ST
            NodeTalk communication = new NodeTalk("replicatedInsert");
            communication.setKey(key);
            communication.setValue(value);

            //String parcel="replicatedInsert-" + key + "-" + value;


            try
            {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), association * 2);
                ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                outputStream.writeObject(communication);

                Log.v(TAG, "object written with message: " + communication.getLanguage() +
                "and source port " + communication.getWhoAmI() + " and destination port " + association);

                Log.v(TAG, "parcel sent with message replicatedInsert to " + association);
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }

        if(s1PortForAssociation!=launchPort)
        {
            Log.v(TAG,"INSERT BLOCK 3");
            NodeTalk communication = new NodeTalk("replicatedInsert");
            communication.setKey(key);
            communication.setValue(value);
            //String parcel="replicatedInsert-" + key + "-" + value;
            try
            {
                Log.v(TAG,"inside send block for successorOnePort");
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), s1PortForAssociation * 2);
                ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                outputStream.writeObject(communication);

                Log.v(TAG, "object written with message: " + communication.getLanguage() +
                      "and source port " + communication.getWhoAmI() + " and destination port " + s1PortForAssociation);
                Log.v(TAG, "parcel sent with message replicatedInsert to " + s1PortForAssociation);
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }

        }
        if(s2PortForAssociation!=launchPort)
        {

            Log.v(TAG,"INSERT BLOCK 4");
            NodeTalk communication = new NodeTalk("replicatedInsert");
            communication.setKey(key);
            communication.setValue(value);

            //String parcel="replicatedInsert-" + key + "-" + value;
            try
            {
                Log.v(TAG,"inside send block for successorOnePort");
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), s2PortForAssociation * 2);
                ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                outputStream.writeObject(communication);

                Log.v(TAG, "object written with message: " + communication.getLanguage() +
                      "and source port " + communication.getWhoAmI() + " and destination port " + s2PortForAssociation);
                Log.v(TAG, "parcel sent with message replicatedInsert to " + s2PortForAssociation);

            }
            catch(Exception e)
            {
                e.getMessage();
            }

        }

        //mutex.unlock();
        return null;
    }


    @Override
    public boolean onCreate()
    {
        Log.v(TAG, "#ENTERING ON CREATE#");
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        launchPort = Integer.parseInt(portStr);

        nodeSpace.add(5554);
        nodeSpace.add(5556);
        nodeSpace.add(5558);
        nodeSpace.add(5560);
        nodeSpace.add(5562);

        int myPosition=0;
        int successorOne=0;
        int successorTwo=0;

        Collections.sort(nodeSpace, new Comparator<Integer>()
        {
            @Override
            public int compare(Integer here, Integer there) {
                int compareDecision = 0;
                try {
                    compareDecision = genHash(Integer.toString(here)).compareTo(genHash(Integer.toString(there)));
                }
                catch (NoSuchAlgorithmException e) {
                    Log.e(TAG, "got exception in comparator");
                }
                return compareDecision;
            }
        });

        //now find self id, successor port 1 and successor port 2 in the array list
        for(int i=0; i<nodeSpace.size(); i++)
        {
            if(nodeSpace.get(i)==launchPort)
            {
                myPosition=i;
                break;
            }
        }
        successorOne=(myPosition+1)%5;
        successorTwo=(myPosition+2)%5;
        successorOnePort=nodeSpace.get(successorOne);
        successorTwoPort=nodeSpace.get(successorTwo);
        Log.v(TAG,"MY PORT: " + launchPort);
        Log.v(TAG,"Successor 1 PORT: " + successorOnePort);
        Log.v(TAG,"Successor 2 PORT: " + successorTwoPort);

        try
        {
            ServerSocket serverSocket = new ServerSocket(serverPort);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }
        catch (IOException e)
        {
            e.getMessage();
            return false;
        }
        NodeTalk identifierToClientTask = new NodeTalk("recover");
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, identifierToClientTask);
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder)
    {
        Log.v(TAG,"inside Cursor Query");
        Cursor cursor=null;
        String[] columns = {"key", "value"};
        MatrixCursor matrixCursor=new MatrixCursor(columns);
        File [] array = new File[1];

        if(selection.equals(lDump))
        {
            Log.v(TAG,"CASE LDUMP");
            contextType=getContext();
            File file = new File(String.valueOf(contextType.getFilesDir()));
            array = file.listFiles();

            for(int i=0; i<array.length;i++)
            {
                String strKey = array[i].getName();
                String msgValue = "";

                try {
                    FileInputStream fis = getContext().openFileInput(strKey);
                    BufferedInputStream bis = new BufferedInputStream(fis);
                    int temp;
                    while ((temp = bis.read()) != -1) {
                        msgValue += (char) temp;
                    }
                    bis.close();
                } catch (IOException ioe) {
                    Log.e("Query", ioe.getMessage());
                } catch (NullPointerException npe) {
                    Log.e("Query", npe.getMessage());
                } catch (Exception e) {
                    Log.e("Query", e.getMessage());
                }

                matrixCursor.addRow(new String[]{strKey, msgValue});
            }
            //Cursor c2=null;
            Cursor c3=null;
            try {
                MergeCursor mergeCursor = new MergeCursor(new Cursor[]{matrixCursor, c3});
                cursor = mergeCursor;
                cursor.moveToFirst();
            }
            catch (NullPointerException e)
            {
                e.printStackTrace();
            }


        }
        else if(selection.equals(gDump))
        {
            Log.v(TAG,"CASE GDUMP");

            contextType=getContext();
            File file = new File(String.valueOf(contextType.getFilesDir()));
            array = file.listFiles();

            for(int i=0; i<array.length;i++)
            {
                String strKey = array[i].getName();
                String msgValue = "";

                try {
                    FileInputStream fis = getContext().openFileInput(strKey);
                    BufferedInputStream bis = new BufferedInputStream(fis);
                    int temp;
                    while ((temp = bis.read()) != -1) {
                        msgValue += (char) temp;
                    }
                    bis.close();
                } catch (IOException ioe) {
                    Log.e("Query", ioe.getMessage());
                } catch (NullPointerException npe) {
                    Log.e("Query", npe.getMessage());
                } catch (Exception e) {
                    Log.e("Query", e.getMessage());
                }

                matrixCursor.addRow(new String[]{strKey, msgValue});
            }


            String hugeString = Integer.toString(launchPort)+ "#";
            Cursor c2=null;
            Cursor c3=null;
            int iterator;



            try {
                MergeCursor mergeCursor = new MergeCursor(new Cursor[]{matrixCursor, c3});
                c2 = mergeCursor;
                c2.moveToFirst();
            }
            catch (NullPointerException e)
            {
                e.printStackTrace();
            }

            Log.d(TAG, "query: Cursor count is : "+c2.getCount());
            if(c2.getCount()>0)
            {
                do
                {
                    hugeString+=c2.getString(0)+"-"+c2.getString(1)+"*";

                }while (c2.moveToNext());
            }
            else
            {
                hugeString+="random-random*";
            }
            hugeString+="@";
            for(int i=0;i<nodeSpace.size();i++)
            {
                iterator=nodeSpace.get(i);
                if(iterator!=launchPort)
                {

                    try {
                        int myContactForLDUMP = nodeSpace.get(i);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), myContactForLDUMP * 2);
                        NodeTalk communication = new NodeTalk("sendYourLDUMP");
                        ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                        outputStream.writeObject(communication);

                        DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                        String processMyContactForLDUMPReply = dataInputStream.readUTF();

                        hugeString+=processMyContactForLDUMPReply+"@";
                        Log.v(TAG,"@@huge string: "+ hugeString);


                    }
                    catch (IOException e)
                    {
                        e.printStackTrace();
                    }

                }
            }

            MatrixCursor mc = new MatrixCursor(columns);

            String[] splitter = hugeString.split("@");
            int i=0;

            while(i<splitter.length)
            {
                String[] splitter2 = splitter[i].split("#");
                Log.v(TAG,"splitter2 at 0: " + splitter2[0] +"splitter2 at 1: "+ splitter2[1]);
                String[] splitter3 = splitter2[1].split("\\*");
                int j=0;
                while(j<splitter3.length)
                {
                    String[] splitter4 = splitter3[j].split("-");
                    if(!splitter4[0].equals("random")){
                        mc.addRow(new String[] {splitter4[0],splitter4[1]});}
                    j++;
                }
                i++;
            }
            try {
                MergeCursor mergeCursor = new MergeCursor(new Cursor[]{mc, c2});
                cursor = mergeCursor;
                cursor.moveToFirst();
            }
            catch (NullPointerException e)
            {
                e.printStackTrace();
            }

        }
        else
        {
            Log.v(TAG, "CASE SPECIFIC KEY : " + selection);
            //calculate association
            String key=selection;
            String msgValue="";
            int association=0;
            Iterator iterator = nodeSpace.iterator();
            while(iterator.hasNext())
            {
                try
                {
                    String string=(iterator.next()).toString();
                    if(genHash(key).compareTo(genHash(string)) <=0)
                    {
                        association=Integer.parseInt(string);
                        break;
                    }
                }
                catch (NoSuchAlgorithmException e)
                {
                    Log.v(TAG,"Exception in key comparison");
                }
            }
            if(association == 0)
            {
                association = nodeSpace.get(0);
            }
            //now find self id, successor port 1 and successor port 2 in the array list
            int myPosition=0;
            int predecessorOne=0;
            int predecessorTwo=0;

            for(int i=0; i<nodeSpace.size(); i++)
            {
                if(nodeSpace.get(i)==launchPort)
                {
                    myPosition=i;
                    break;
                }
            }
            predecessorOne=(myPosition+4)%5;
            predecessorTwo=(myPosition+3)%5;
            predecessorOnePort=nodeSpace.get(predecessorOne);
            predecessorTwoPort=nodeSpace.get(predecessorTwo);
            Log.v(TAG,"MY PORT: " + launchPort);
            Log.v(TAG,"Predecessor 1 PORT: " + predecessorOnePort);
            Log.v(TAG,"Predecessor 2 PORT: " + predecessorTwoPort);

            if(association==launchPort || association==predecessorOnePort || association==predecessorTwoPort) {
                Log.v(TAG, "local query for specific key: CASE 1 " + key);

                try {
                    FileInputStream fis = getContext().openFileInput(selection);
                    BufferedInputStream bis= new BufferedInputStream(fis);
                    int temp;
                    while((temp= bis.read())!=-1){
                        msgValue+= (char)temp;
                    }
                    bis.close();
                }catch(IOException ioe){
                    Log.e("Query",ioe.getMessage());
                }catch(NullPointerException npe){
                    Log.e("Query",npe.getMessage());
                }catch(Exception e){
                    Log.e("Query",e.getMessage());
                }
                String[] columnNames= {"key", "value"};
                MatrixCursor msgCursor= new MatrixCursor(columnNames);
                msgCursor.addRow(new String[]{selection, msgValue});

                Cursor c2=null;

                try
                {
                    MergeCursor mergeCursor = new MergeCursor(new Cursor[]{msgCursor, c2});
                    cursor = mergeCursor;
                    cursor.moveToFirst();
                }
                catch (NullPointerException e)
                {
                    e.printStackTrace();
                }

            }
            else
            {
                try
                {
                    Log.v(TAG, "Call to server of association");
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), association * 2);
                    NodeTalk communication = new NodeTalk("specificKeyOther");
                    communication.setKey(selection);
                    Log.v(TAG, "key from query function: " + selection);
                    communication.setWhoAmI(launchPort);
                    ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                    outputStream.writeObject(communication);


                    DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                    String processDaemonReply = dataInputStream.readUTF();
                    Log.v(TAG,"processDaemonReply is: " +  processDaemonReply);

                    if(processDaemonReply!=null)
                    {
                        Log.v(TAG,"daemon reply processed for redirected query as : " + processDaemonReply);
                        String[] tokenContainer = processDaemonReply.split("-");

                        try {
                            matrixCursor.addRow(new String[]{tokenContainer[1], tokenContainer[2]});
                            MergeCursor mergeCursor = new MergeCursor(new Cursor[]{matrixCursor, cursor});
                            cursor = mergeCursor;
                            cursor.moveToFirst();
                            Log.v(TAG, "processDaemonReply as: " + cursor.getString(0) +"-"+ cursor.getString(1));
                        }
                        catch (Exception e)
                        {
                            e.printStackTrace();
                        }
                    }
                }
                catch(IOException e)
                {
                    e.printStackTrace();
                    int hop=0;

                    for(int i=0; i<nodeSpace.size(); i++)
                    {
                        if(nodeSpace.get(i)==association)
                        {
                            hop=i;
                            break;
                        }
                    }
                    int sInd = (hop+1)%5;
                    int sPort = nodeSpace.get(sInd);
                    try {
                        Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), sPort * 2);
                        NodeTalk communication2 = new NodeTalk("specificKeyOther");
                        communication2.setKey(selection);
                        Log.v(TAG, "key from query function: " + selection);
                        communication2.setWhoAmI(launchPort);
                        ObjectOutputStream outputStream2 = new ObjectOutputStream(socket2.getOutputStream());
                        outputStream2.writeObject(communication2);

                        DataInputStream dataInputStream2 = new DataInputStream(socket2.getInputStream());
                        String processDaemonReply2 = dataInputStream2.readUTF();


                        if(processDaemonReply2!=null)
                        {
                            Log.v(TAG,"daemon reply received for redirected query as : " + processDaemonReply2);
                            String[] tokenContainer = processDaemonReply2.split("-");

                            try {
                                matrixCursor.addRow(new String[]{tokenContainer[1], tokenContainer[2]});
                                MergeCursor mergeCursor = new MergeCursor(new Cursor[]{matrixCursor, cursor});
                                cursor = mergeCursor;
                                cursor.moveToFirst();
                                Log.v(TAG, "Daemon Reply processed as: " + cursor.getString(0) +"-"+ cursor.getString(1));
                            }
                            catch (Exception t)
                            {
                                t.printStackTrace();
                            }
                        }

                    }
                    catch(IOException o)
                    {
                        o.printStackTrace();
                    }

                }

            }

        }

        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs)
    {
        // TODO Auto-generated method stub
        return 0;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void>
    {
        Socket socket = null;
        @Override
        protected Void doInBackground(ServerSocket... sockets)
        {
            Log.v(TAG, "%%%launch port is: " + launchPort);
            ServerSocket serverSocket = sockets[0];

            try
            {
                String daemonReply="";
                while(true)
                {
                    Log.v(TAG,"#SERVER " + launchPort + " LISTENING FOR INCOMING CONNECTIONS#");
                    socket=serverSocket.accept();

                    ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
                    NodeTalk communication = (NodeTalk)inputStream.readObject();

                    if(communication!=null)
                    {
                        if(communication.getLanguage().equals("replicatedInsert"))
                        {
                            Log.v(TAG, " Language type is replicatedInsert ");
                            daemonReply = insertHandler(communication);
                        }
                        if(communication.getLanguage().equals("specificKeyOther"))
                        {
                            Log.v(TAG, " Language type is specificKeyOther");
                            daemonReply = specificKeyHandler(communication);
                            try
                            {
                                DataOutputStream dataOutPutStream = new DataOutputStream(socket.getOutputStream());
                                dataOutPutStream.writeUTF(daemonReply);
                                Log.v(TAG, "daemonReply written to output stream for Language Type: " + communication.getLanguage());
                            }
                            catch (IOException e)
                            {
                                e.printStackTrace();
                            }
                        }
                        if(communication.getLanguage().equals("sendYourLDUMP"))
                        {
                            Log.v(TAG, " Language type is sendYourLDUMP");
                            daemonReply = sendYourLDUMPHandler();
                            try
                            {
                                DataOutputStream dataOutPutStream = new DataOutputStream(socket.getOutputStream());
                                dataOutPutStream.writeUTF(daemonReply);
                                Log.v(TAG, "daemonReply written to output stream for Language Type: " + communication.getLanguage());
                            }
                            catch (IOException e)
                            {
                                e.printStackTrace();
                            }
                        }
                        if(communication.getLanguage().equals("forSuccessor"))
                        {
                            Log.v(TAG, " Language type is forSuccessor");
                            daemonReply = forSuccessorHandler(communication);
                            try
                            {
                                DataOutputStream dataOutPutStream = new DataOutputStream(socket.getOutputStream());
                                dataOutPutStream.writeUTF(daemonReply);
                                Log.v(TAG, "daemonReply written to output stream for Language Type: " + communication.getLanguage());
                            }
                            catch (IOException e)
                            {
                                e.printStackTrace();
                            }
                        }
                        if(communication.getLanguage().equals("forPPD"))
                        {
                            Log.v(TAG, " Language type is forPPD");
                            daemonReply = forPPDHandler(communication);
                            try
                            {
                                DataOutputStream dataOutPutStream = new DataOutputStream(socket.getOutputStream());
                                dataOutPutStream.writeUTF(daemonReply);
                                Log.v(TAG, "daemonReply written to output stream for Language Type: " + communication.getLanguage());
                            }
                            catch (IOException e)
                            {
                                e.printStackTrace();
                            }
                        }
                        if(communication.getLanguage().equals("distributedDelete"))
                        {
                            Log.v(TAG, "Language type is distributedDelete");
                            distributedDeleteHandler(communication);
                        }

                    }

                }
            }
            catch(ClassNotFoundException c)
            {
                c.printStackTrace();
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }

            return null;
        }

        String insertHandler(NodeTalk nodeTalk)
        {
            ContentValues values = new ContentValues();
            String daemonReply="";

            try
            {
                FileOutputStream fos = getContext().openFileOutput(nodeTalk.getKey(), Context.MODE_PRIVATE);
                fos.write(nodeTalk.getValue().getBytes());
                fos.close();
            }catch(IOException ioe){
                Log.e("Insert", ioe.getMessage());
            }catch(NullPointerException npe){
                Log.e("Insert", npe.getMessage());
            }catch(Exception e){
                Log.e("Insert", e.getMessage());
            }


            daemonReply="Insert successful";
            return daemonReply;
        }


        String specificKeyHandler(NodeTalk nodeTalk)
        {
            Cursor cursor = null;
            String daemonReply="";
            String key = nodeTalk.getKey();
            String msgValue="";
            Log.v(TAG, "local query for specific key: " + key);

            try {
                FileInputStream fis = getContext().openFileInput(key);
                BufferedInputStream bis= new BufferedInputStream(fis);
                int temp;
                while((temp= bis.read())!=-1){
                    msgValue+= (char)temp;
                }
                bis.close();
            }catch(IOException ioe){
                Log.e("Query",ioe.getMessage());
            }catch(NullPointerException npe){
                Log.e("Query",npe.getMessage());
            }catch(Exception e){
                Log.e("Query",e.getMessage());
            }
            String[] columnNames= {"key", "value"};
            MatrixCursor msgCursor= new MatrixCursor(columnNames);
            msgCursor.addRow(new String[]{key, msgValue});

            Cursor c2=null;

            try
            {
                MergeCursor mergeCursor = new MergeCursor(new Cursor[]{msgCursor, c2});
                cursor = mergeCursor;
                cursor.moveToFirst();
            }
            catch (NullPointerException e)
            {
                e.printStackTrace();
            }

            Log.v(TAG, "key at specificKeyHandler: " + key);
            String value = cursor.getString(1);
            daemonReply = "singlequery-" + key + "-" + value;
            Log.v(TAG, "specificKeyHandler returned: " + daemonReply);


            return daemonReply;
        }
        String sendYourLDUMPHandler()
        {
            Cursor cursor = null;
            String[] columns = {"key","value"};
            File array[] = new File[1];
            MatrixCursor matrixCursor = new MatrixCursor(columns);

            Log.v(TAG,"CASE LDUMP IN LDUMPHANDLER");
            contextType=getContext();
            File file = new File(String.valueOf(contextType.getFilesDir()));
            array = file.listFiles();

            for(int i=0; i<array.length;i++)
            {
                String strKey = array[i].getName();
                String msgValue = "";

                try {
                    FileInputStream fis = getContext().openFileInput(strKey);
                    BufferedInputStream bis = new BufferedInputStream(fis);
                    int temp;
                    while ((temp = bis.read()) != -1) {
                        msgValue += (char) temp;
                    }
                    bis.close();
                } catch (IOException ioe) {
                    Log.e("Query", ioe.getMessage());
                } catch (NullPointerException npe) {
                    Log.e("Query", npe.getMessage());
                } catch (Exception e) {
                    Log.e("Query", e.getMessage());
                }

                matrixCursor.addRow(new String[]{strKey, msgValue});
            }

            Cursor c3=null;
            try {
                MergeCursor mergeCursor = new MergeCursor(new Cursor[]{matrixCursor, c3});
                cursor = mergeCursor;
                cursor.moveToFirst();
            }
            catch (NullPointerException e)
            {
                e.printStackTrace();
            }

            String hugeString=Integer.toString(launchPort)+ "#";
            if(cursor.getCount()>0)
            {
                do
                {
                    hugeString+=cursor.getString(0)+"-"+cursor.getString(1)+"*";

                }while(cursor.moveToNext());
            }
            else
            {
                hugeString += "random-random*";
            }
            Log.v(TAG, "huge string sent from ST :" + hugeString);
            return  hugeString;
        }
        String forPPDHandler(NodeTalk nodeTalk)
        {
            Cursor cursor=null;
            String hugeString="myldump&";
            String[] columns = {"key", "value"};
            MatrixCursor matrixCursor=new MatrixCursor(columns);

            File array[] = new File[1];

            Log.v(TAG, "CASE LDUMP");
            contextType=getContext();
            File file = new File(String.valueOf(contextType.getFilesDir()));
            array = file.listFiles();

            for(int i=0; i<array.length;i++)
            {
                String strKey = array[i].getName();
                String msgValue = "";

                try {
                    FileInputStream fis = getContext().openFileInput(strKey);
                    BufferedInputStream bis = new BufferedInputStream(fis);
                    int temp;
                    while ((temp = bis.read()) != -1) {
                        msgValue += (char) temp;
                    }
                    bis.close();
                } catch (IOException ioe) {
                    Log.e("Query", ioe.getMessage());
                } catch (NullPointerException npe) {
                    Log.e("Query", npe.getMessage());
                } catch (Exception e) {
                    Log.e("Query", e.getMessage());
                }

                matrixCursor.addRow(new String[]{strKey, msgValue});
            }

            Cursor c3=null;
            try {
                MergeCursor mergeCursor = new MergeCursor(new Cursor[]{matrixCursor, c3});
                cursor = mergeCursor;
                cursor.moveToFirst();
            }
            catch (NullPointerException e)
            {
                e.printStackTrace();
            }


            int association=0;
            Log.v(TAG,"Recover request for ppd recvd at " + launchPort + " from " + nodeTalk.getWhoAmI());

            if(cursor.getCount()>0)
            {
                do
                {
                    Iterator iterator = nodeSpace.iterator();
                    while(iterator.hasNext())
                    {
                        try
                        {
                            String string=(iterator.next()).toString();
                            if(genHash(cursor.getString(0)).compareTo(genHash(string)) <=0)
                            {
                                association=Integer.parseInt(string);
                                break;
                            }
                        }
                        catch (NoSuchAlgorithmException e)
                        {
                            Log.v(TAG,"Exception in key comparison");
                        }
                    }
                    if(association == 0)
                    {
                        association = nodeSpace.get(0);
                    }

                    if(association==launchPort)
                    {
                        hugeString += cursor.getString(0) + "-" + cursor.getString(1) + "&";
                    }

                }while(cursor.moveToNext());
            }
            Log.v(TAG, "HUGE STRING sent from ST [CASE forPPDHandler] : " + hugeString + " to " + nodeTalk.getWhoAmI());
            return  hugeString;

        }
        String forSuccessorHandler(NodeTalk nodeTalk)
        {

            Cursor cursor=null;
            String[] columns = {"key", "value"};
            MatrixCursor matrixCursor=new MatrixCursor(columns);
            int myPosition=0;
            int firstPosition=0;
            int firstPort=0;
            String hugeString2="myxdump&";

            File array[] = new File[1];


            contextType=getContext();
            File file = new File(String.valueOf(contextType.getFilesDir()));
            array = file.listFiles();

            for(int i=0; i<array.length;i++)
            {
                String strKey = array[i].getName();
                String msgValue = "";

                try {
                    FileInputStream fis = getContext().openFileInput(strKey);
                    BufferedInputStream bis = new BufferedInputStream(fis);
                    int temp;
                    while ((temp = bis.read()) != -1) {
                        msgValue += (char) temp;
                    }
                    bis.close();
                } catch (IOException ioe) {
                    Log.e("Query", ioe.getMessage());
                } catch (NullPointerException npe) {
                    Log.e("Query", npe.getMessage());
                } catch (Exception e) {
                    Log.e("Query", e.getMessage());
                }

                matrixCursor.addRow(new String[]{strKey, msgValue});
            }

            Cursor c3=null;
            try {
                MergeCursor mergeCursor = new MergeCursor(new Cursor[]{matrixCursor, c3});
                cursor = mergeCursor;
                cursor.moveToFirst();
            }
            catch (NullPointerException e)
            {
                e.printStackTrace();
            }

            int association=0;

            Log.v(TAG,"Recover request for successor recvd at " + launchPort + " from " + nodeTalk.getWhoAmI());

            for(int i=0; i<nodeSpace.size(); i++)
            {
                if(nodeSpace.get(i)==nodeTalk.getWhoAmI())
                {
                    myPosition=i;
                    break;
                }
            }

            firstPosition=(myPosition+4)%5;
            firstPort=nodeSpace.get(firstPosition);


            if(cursor.getCount()>0)
            {
                do
                {
                    Iterator iterator = nodeSpace.iterator();
                    while(iterator.hasNext())
                    {
                        try
                        {
                            String string=(iterator.next()).toString();
                            if(genHash(cursor.getString(0)).compareTo(genHash(string)) <=0)
                            {
                                association=Integer.parseInt(string);
                                break;
                            }
                        }
                        catch (NoSuchAlgorithmException e)
                        {
                            Log.v(TAG,"Exception in key comparison");
                        }
                    }
                    if(association == 0)
                    {
                        association = nodeSpace.get(0);
                    }


                    if(association==nodeTalk.getWhoAmI() || association == firstPort)
                    {
                        hugeString2 += cursor.getString(0) + "-" + cursor.getString(1) + "&";
                    }

                }while(cursor.moveToNext());
            }
            Log.v(TAG, "HUGE STRING sent from ST [CASE forSuccessorHandler] : " + hugeString2 + "to " + nodeTalk.getWhoAmI());
            return hugeString2;
        }
        void distributedDeleteHandler(NodeTalk nodeTalk)
        {
            String key=nodeTalk.getKey();
            try
            {
                contextType.deleteFile(key);
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }



    }

    private class ClientTask extends AsyncTask<NodeTalk, Void, Cursor>
    {
        @Override
        protected Cursor doInBackground(NodeTalk... params)
        {

            Log.v(TAG,"Inside doInBackground of CT");
            try
            {
                NodeTalk obj = params[0];
                if(obj.getLanguage().equals("recover"))
                {
                    Log.v(TAG,"Inside recover block at ClientTAsk");
                    int successorIndex=0;
                    int ppdIndex=0;
                    int successorP=0;
                    int ppdP=0;
                    int myPosition=0;

                    for(int i=0; i<nodeSpace.size(); i++)
                    {
                        if(nodeSpace.get(i)==launchPort)
                        {
                            myPosition=i;
                            break;
                        }
                    }
                    successorIndex=(myPosition+1)%5;
                    ppdIndex=(myPosition+3)%5;
                    successorP=nodeSpace.get(successorIndex);
                    ppdP=nodeSpace.get(ppdIndex);

                    Log.v(TAG,"successor Port is = "+successorP);
                    Log.v(TAG,"pdp Port is = "+ppdP);

                    try
                    {
                        NodeTalk obj2 = new NodeTalk("forPPD");
                        obj2.setWhoAmI(launchPort);

                        Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), ppdP * 2);
                        ObjectOutputStream outputStream2 = new ObjectOutputStream(socket2.getOutputStream());
                        outputStream2.writeObject(obj2);

                        DataInputStream dataInputStream1 = new DataInputStream(socket2.getInputStream());
                        String reply2 = dataInputStream1.readUTF();

                        Log.v(TAG, "RECOVER CASE reply received from PPD  [" + ppdP +"]" + " : " + reply2);

                        if(reply2!=null)
                        {
                            String[] tokenContainer = reply2.split("&");
                            for(int i=1;i<tokenContainer.length;i++)
                            {
                                ContentValues values = new ContentValues();
                                String[] random = tokenContainer[i].split("-");


                                try
                                {
                                    FileOutputStream fos = getContext().openFileOutput(random[0], Context.MODE_PRIVATE);
                                    fos.write(random[1].getBytes());
                                    fos.close();
                                }catch(IOException ioe){
                                    Log.e("Insert", ioe.getMessage());
                                }catch(NullPointerException npe){
                                    Log.e("Insert", npe.getMessage());
                                }catch(Exception e){
                                    Log.e("Insert", e.getMessage());
                                }

                            }
                        }
                    }
                    catch(IOException r)
                    {
                        r.printStackTrace();
                    }

                    try
                    {
                        NodeTalk obj1 = new NodeTalk("forSuccessor");
                        obj1.setWhoAmI(launchPort);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), successorP * 2);
                        ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                        outputStream.writeObject(obj1);

                        DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                        String reply = dataInputStream.readUTF();
                        Log.v(TAG, "RECOVER CASE reply received from Successor [" + successorP +"]" + " : " + reply);

                        if(reply!=null)
                        {
                            String[] tokenContainer = reply.split("&");
                            for(int i=1;i<tokenContainer.length;i++)
                            {
                                ContentValues values = new ContentValues();
                                String[] random = tokenContainer[i].split("-");

                                try
                                {
                                    FileOutputStream fos = getContext().openFileOutput(random[0], Context.MODE_PRIVATE);
                                    fos.write(random[1].getBytes());
                                    fos.close();
                                }catch(IOException ioe){
                                    Log.e("Insert", ioe.getMessage());
                                }catch(NullPointerException npe){
                                    Log.e("Insert", npe.getMessage());
                                }catch(Exception e){
                                    Log.e("Insert", e.getMessage());
                                }

                            }
                        }

                    }
                    catch(IOException e)
                    {
                        e.printStackTrace();
                    }

                }

            }
            catch(Exception e)
            {
                e.printStackTrace();
            }

            return null;
        }
    }

    private String genHash(String input) throws NoSuchAlgorithmException
    {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
