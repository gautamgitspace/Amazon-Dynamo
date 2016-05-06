package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
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
    public ReentrantLock tijori =  new ReentrantLock();
    public ReentrantLock tijori2 = new ReentrantLock();
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs)
	{
		// TODO Auto-generated method stub
        String key=selection;
        DBHandler dbHandler = new DBHandler(getContext());
        SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();
        sqLiteDatabase.delete("dynamoDB", "key=?", new String[] {key});
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
		// TODO Auto-generated method stub
        tijori.lock();
        String key = (String)values.get("key");
        String value = (String)values.get("value");
        int association=0;

        //send insert request to ST. ST Stores it.

        /*Sorting array list based on comparator - http://www.tutorialspoint.com/java/java_using_comparator.htm*/
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
        Log.v(TAG, "Key: " + key + "associated with: " + association);

        if(association==launchPort)
        {
            //LOCAL INSERT
            Log.v("TAG", "insert locally");
            Log.v(TAG,"Key: " + key + "inserted locally at" + association);
            DBHandler dbHandler = new DBHandler(getContext());
            SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();
            values.put("key", key);
            values.put("value",value);
            sqLiteDatabase.insert("dynamoDB", null, values);
            sqLiteDatabase.close();

            //PREPARE OBJECT FOR REPLICATED INSERT and SEND TO ST
            NodeTalk communication = new NodeTalk("replicatedInsert");
            communication.setSuccessor1(successorOnePort);
            communication.setSuccessor2(successorTwoPort);
            communication.setKey(key);
            communication.setValue(value);
            communication.setWhoAmI(launchPort);
            try
            {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), successorOnePort * 2);
                ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                Log.v(TAG, "about to write" + communication + "to stream");
                outputStream.writeObject(communication);
                Log.v(TAG, "object written with message: " + communication.getLanguage() +
                        "and source port " + communication.getWhoAmI() + "and destination port " + communication.getSuccessor1());
            }
            catch(Exception e)
            {
                e.getMessage();
            }

            try {

                Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), successorTwoPort * 2);
                ObjectOutputStream outputStream1 = new ObjectOutputStream(socket1.getOutputStream());
                Log.v(TAG, "about to write" + communication + "to stream");
                outputStream1.writeObject(communication);
                Log.v(TAG, "object written with message: " + communication.getLanguage() +
                        "and source port " + communication.getWhoAmI() + "and destination port " + communication.getSuccessor2());
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
            Log.v(TAG, "Replicated Insert call sent to CT");
        }
        else if(association!=launchPort)
        {
            int s1IndexforAssociation=0;
            int s2IndexforAssociation=0;
            int s1PortforAssociation=0;
            int s2PortforAssociation=0;
            int associationIndex=0;

            for(int i=0; i<nodeSpace.size(); i++)
            {
                if(nodeSpace.get(i)==association)
                {
                    associationIndex=i;
                    break;
                }
            }
            s1IndexforAssociation=(associationIndex+1)%5;
            s2IndexforAssociation=(associationIndex+2)%5;
            s1PortforAssociation=nodeSpace.get(s1IndexforAssociation);
            s2PortforAssociation=nodeSpace.get(s2IndexforAssociation);


            if(s1PortforAssociation==launchPort || s2PortforAssociation==launchPort)
            {
                DBHandler dbHandler = new DBHandler(getContext());
                SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();
                values.put("key", key);
                values.put("value",value);
                sqLiteDatabase.insert("dynamoDB", null, values);
                sqLiteDatabase.close();
            }

            //PREPARE OBJECT FOR REDIRECTED REPLICATED INSERT
            Log.v(TAG, "redirected insert");
            NodeTalk communication = new NodeTalk("redirectedInsert");
            communication.setKey(key);
            communication.setValue(value);
            communication.setWhoAmI(launchPort);
            communication.setAssociation(association);
            //CALL TO ST
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), association * 2);
                ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                Log.v(TAG, "about to write" + communication + "to stream");
                outputStream.writeObject(communication);
                Log.v(TAG, "object written with message: " + communication.getLanguage() +
                        "and source port" + communication.getWhoAmI() + "and destination port" + communication.getAssociation());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        tijori.unlock();
		return null;
	}

	@Override
	public boolean onCreate()
	{
		// TODO Auto-generated method stub

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        launchPort = Integer.parseInt(portStr);

        nodeSpace.add(5554);
        nodeSpace.add(5556);
        nodeSpace.add(5558);
        nodeSpace.add(5560);
        nodeSpace.add(5562);

        int myIndex=0;
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
                myIndex=i;
                break;
            }
        }
        successorOne=(myIndex+1)%5;
        successorTwo=(myIndex+2)%5;
        successorOnePort=nodeSpace.get(successorOne);
        successorTwoPort=nodeSpace.get(successorTwo);
        Log.v(TAG,"MY PORT: " + launchPort);
        Log.v(TAG,"S1 PORT: " + successorOnePort);
        Log.v(TAG,"S2 PORT: " + successorTwoPort);


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
        return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder)
	{
		// TODO Auto-generated method stub
        Log.v(TAG,"inside Cursor query");
        Cursor cursor=null;
        String[] columns = {"key", "value"};
        MatrixCursor matrixCursor=new MatrixCursor(columns);

        if(selection.equals(lDump))
        {
            Log.v(TAG,"CASE LDUMP");
            DBHandler dbHandler = new DBHandler(getContext());
            SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();
            cursor = sqLiteDatabase.query(true, "dynamoDB", columns, null, null, null, null, null, null);
            cursor.moveToFirst();
        }
        else if(selection.equals(gDump))
        {
            Log.v(TAG,"CASE GDUMP");

            int iterator;
            DBHandler dbHandler = new DBHandler(getContext());
            SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();
            String hugeString = Integer.toString(launchPort)+ "#";
            Cursor c2=null;
            c2 = sqLiteDatabase.query(true, "dynamoDB", columns, null, null, null, null, null, null);
            //cursor.moveToFirst();
                            /* http://stackoverflow.com/questions/2810615/how-to-retrieve-data-from-cursor-class */
            c2.moveToFirst();
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
                    //send message to nodeSpace.get(i)
                    try {
                        int myContactforLDUMP = nodeSpace.get(i);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), myContactforLDUMP * 2);
                        NodeTalk communication = new NodeTalk("sendYourLDUMP");
                        ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                        outputStream.writeObject(communication);

                        DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                        String processMyContactforLDUMPReply = dataInputStream.readUTF();

                        hugeString+=processMyContactforLDUMPReply+"@";
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
           //tijori2.lock();
            Log.v(TAG, "CASE SPECIFIC KEY : " + selection);
            //calculate association
            String key=selection;
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
            int myIndex=0;
            int predecessorOne=0;
            int predecessorTwo=0;

            for(int i=0; i<nodeSpace.size(); i++)
            {
                if(nodeSpace.get(i)==launchPort)
                {
                    myIndex=i;
                    break;
                }
            }
            predecessorOne=(myIndex+4)%5;
            predecessorTwo=(myIndex+3)%5;
            predecessorOnePort=nodeSpace.get(predecessorOne);
            predecessorTwoPort=nodeSpace.get(predecessorTwo);
            Log.v(TAG,"MY PORT: " + launchPort);
            Log.v(TAG,"S1 PORT: " + predecessorOnePort);
            Log.v(TAG,"S2 PORT: " + predecessorTwoPort);

            if(association==launchPort || association==predecessorOnePort || association==predecessorTwoPort)
            {
                Log.v(TAG,"local query for specific key: CASE 1 " + key);
                DBHandler dbHandler = new DBHandler(getContext());
                SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();
                cursor = sqLiteDatabase.query(true, "dynamoDB", columns, "key=?", new String[]{selection}, null, null, null, null);
            }
            else
            {
                try
                {
                    Log.v(TAG, "Call to server of association");
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), association * 2);
                    NodeTalk communication = new NodeTalk("specificKeyOther");
                    communication.setKey(selection);
                    Log.v(TAG,"key from query function: " + selection);
                    communication.setWhoAmI(launchPort);
                    ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                    outputStream.writeObject(communication);
                    Log.v(TAG, " specificKeyOther sent to ST on socket " + socket.toString());


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
                }
                finally
                {
                 //tijori2.unlock();
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
                    Log.v(TAG,"#SERVER" + launchPort + "LISTENING FOR INCOMING CONNECTIONS#");
                    socket=serverSocket.accept();
                    ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
                    NodeTalk communication = (NodeTalk)inputStream.readObject();

                    if(communication!=null)
                    {
                        if(communication.getLanguage().equals("replicatedInsert"))
                        {
                            Log.v(TAG, " Language type is replicatedInsert ");
                            daemonReply = insertHandler(communication);
                            Log.v(TAG, "Response code [1] to client sent");
                        }
                        if(communication.getLanguage().equals("redirectedInsert"))
                        {
                            Log.v(TAG, " Language type is redirectedInsert");
                            daemonReply = redirectedInsertHandler(communication);
                            Log.v(TAG, "Response code [2] to client sent");
                        }
                        if(communication.getLanguage().equals("redirectedReplicatedInsert"))
                        {
                            Log.v(TAG, " Language type is redirectedInsert");
                            daemonReply = redirectedReplicatedInsertHandler(communication);
                            Log.v(TAG, "Response code [3] to client sent");
                        }
                        if(communication.getLanguage().equals("specificKeyOther"))
                        {
                            Log.v(TAG, " Language type is specificKeyOther");
                            daemonReply = specificKeyHandler(communication);
                        }
                        if(communication.getLanguage().equals("sendYourLDUMP"))
                        {
                            Log.v(TAG, " Language type is sendYourLDUMP");
                            daemonReply = sendYourLDUMPHandler(communication);
                            Log.v(TAG, " sendYourLDUMP Response to client sent");
                        }
                        try
                        {
                            Log.v(TAG, "daemonReply sent from server on socket :" + socket.toString());
                            DataOutputStream dataOutPutStream = new DataOutputStream(socket.getOutputStream());
                            dataOutPutStream.writeUTF(daemonReply);
                        }
                        catch (IOException e)
                        {
                            e.printStackTrace();
                        }
                    }

                }
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
            catch(ClassNotFoundException c)
            {
                c.getMessage();
            }


            return null;
        }

        String insertHandler(NodeTalk nodeTalk)
        {
            ContentValues values = new ContentValues();
            String daemonReply="";
            DBHandler dbHandler = new DBHandler(getContext());
            SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();

            values.put("key",nodeTalk.getKey());
            values.put("value", nodeTalk.getValue());
            Log.v(TAG, "Key: " + nodeTalk.getKey() + "inserted at" + launchPort);
            sqLiteDatabase.insert("dynamoDB", null, values);
            sqLiteDatabase.close();
            daemonReply="Insert successful";
            return daemonReply;
        }

        String redirectedInsertHandler(NodeTalk nodeTalk)
        {
            ContentValues values = new ContentValues();
            String daemonReply="";
            boolean sendFlag1 = true;
            boolean sendFlag2 =true;

            //LOCAL INSERT
            Log.v("TAG", "insert locally");
            DBHandler dbHandler = new DBHandler(getContext());
            SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();
            values.put("key", nodeTalk.getKey());
            values.put("value",nodeTalk.getValue());
            Log.v(TAG, "Key: " + nodeTalk.getKey() + "inserted at" + launchPort);
            sqLiteDatabase.insert("dynamoDB", null, values);
            sqLiteDatabase.close();


            //REPLICATE ON SUCCESSORS
            if(successorOnePort==nodeTalk.getWhoAmI())
            {
                sendFlag1=false;
            }
            if(successorTwoPort==nodeTalk.getWhoAmI())
            {
                sendFlag2=false;
            }

            if(sendFlag1==true)
            {
                try
                {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), successorOnePort * 2);
                    NodeTalk communication = new NodeTalk("redirectedReplicatedInsert");
                    communication.setKey(nodeTalk.getKey());
                    communication.setValue(nodeTalk.getValue());
                    ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                    outputStream.writeObject(communication);
                    Log.v(TAG, "Replicate to S1");
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
            if(sendFlag2==true)
            {

                try {
                    Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), successorTwoPort * 2);
                    NodeTalk communication1 = new NodeTalk("redirectedReplicatedInsert");
                    communication1.setKey(nodeTalk.getKey());
                    communication1.setValue(nodeTalk.getValue());
                    ObjectOutputStream outputStream1 = new ObjectOutputStream(socket1.getOutputStream());
                    outputStream1.writeObject(communication1);
                    Log.v(TAG, "Replicate to S2");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }


                daemonReply = "Redirected Insert successful";
                return daemonReply;
        }
        String redirectedReplicatedInsertHandler(NodeTalk nodeTalk)
        {
            ContentValues values = new ContentValues();
            String daemonReply="";
            DBHandler dbHandler = new DBHandler(getContext());
            SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();

            values.put("key",nodeTalk.getKey());
            values.put("value", nodeTalk.getValue());
            Log.v(TAG, "Key: " + nodeTalk.getKey() + "inserted at" + launchPort);
            sqLiteDatabase.insert("dynamoDB", null, values);
            sqLiteDatabase.close();
            daemonReply="Redirect Insert Replicated successfully";
            return daemonReply;
        }
        String specificKeyHandler(NodeTalk nodeTalk)
        {
            Cursor cursor = null;
            String daemonReply="";
            String key = nodeTalk.getKey();
            String[] columns = {"key", "value"};
            Log.v(TAG, "local query for specific key: " + key);
            DBHandler dbHandler = new DBHandler(getContext());
            SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();
            cursor = sqLiteDatabase.query(true, "dynamoDB", columns, "key=?", new String[]{key}, null, null, null, null);

            try {
                Log.v(TAG, "key at handler: " + key);
                cursor.moveToFirst();
                String value = cursor.getString(1);
                daemonReply = "singlequery-" + key + "-" + value;
                Log.v(TAG, "daemon reply is: " + daemonReply);
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
            Log.d(TAG, "redirectedQueryHandler: reply from redirect query is :"+ daemonReply);

            return daemonReply;
        }
        String sendYourLDUMPHandler(NodeTalk nodetalk)
        {
            //TODO
            Cursor cursor = null;
            String[] columns = {"key","value"};
            DBHandler dbHandler = new DBHandler(getContext());
            SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();
            cursor = sqLiteDatabase.query(true, "dynamoDB", columns, null, null, null, null, null, null);
            String hugeString=Integer.toString(launchPort)+ "#";
            cursor.moveToFirst();
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
