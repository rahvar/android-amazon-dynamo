package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Pattern;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	String TAG = SimpleDynamoProvider.class.getSimpleName();
	String currNode = "";
	String currPort = "";
	String predNode = "";
	String succNode = "";
	HashMap<String,String> insertFlagMap = new HashMap<String, String>();

	HashMap<String,Node> nodeTable = new HashMap<String, Node>();
	BlockingQueue<String> bq = new ArrayBlockingQueue<String>(5);



	ContentResolver contentProvider;
	Uri gUri;

	/**
	 * ATTENTION: This was auto-generated to implement the App Indexing API.
	 * See https://g.co/AppIndexing/AndroidStudio for more information.
	 */


	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}


	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		currNode = portStr;
		currPort = myPort;

		nodeTable.put("5554",new Node("5554","5558","5556"));
		nodeTable.put("5556",new Node("5556","5554","5562"));
		nodeTable.put("5558",new Node("5558","5560","5554"));
		nodeTable.put("5560",new Node("5560","5562","5558"));
		nodeTable.put("5562",new Node("5562","5556","5560"));


		predNode = nodeTable.get(currNode).getPred();
		succNode = nodeTable.get(currNode).getSucc();

		contentProvider = getContext().getContentResolver();
		gUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");


		try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             *
             * AsyncTask is a simplified thread construct that Android provides. Please make sure
             * you know how it works by reading
             * http://developer.android.com/reference/android/os/AsyncTask.html
             */
			ServerSocket serverSocket = new ServerSocket(10000);


			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

			//new DeliverTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort);
		} catch (IOException e) {
            /*
             * Log is a good way to debug your code. LogCat prints out all the messages that
             * Log class writes.
             *
             * Please read http://developer.android.com/tools/debugging/debugging-projects.html
             * and http://developer.android.com/tools/debugging/debugging-log.html
             * for more information on debugging.
             */
			Log.e(TAG, "Can't create a ServerSocket");
			//return;
		}






		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"Testing");

		//sendPkt("Testing","11108","TestAck","TestLog");


		return false;


	}




	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			BufferedReader reader;
			//serverSocket.toString();

			String data = "";

			try {

				while (true) {

					Socket clientSocket = serverSocket.accept();



					BufferedReader in = new BufferedReader(
							new InputStreamReader(clientSocket.getInputStream()));

					data = in.readLine();

					DataOutputStream outStream = new DataOutputStream(clientSocket.getOutputStream());

					if(data.startsWith("insert")){
						Log.d(TAG,"ReceivedInsertMessage: "+data);

						String[] info = data.split(":");
						String key = info[1].trim();


						ContentValues cv = new ContentValues();

						String value = info[2].trim();

						insertFlagMap.put(key+":"+value,"y");
						cv.put("key", key);
						cv.put("value", value);
						contentProvider.insert(gUri, cv);

						outStream.writeBytes("InsertAck\n");



					}
					if(data.startsWith("query")){
						Log.d(TAG,"RecievedQueryMessage");

						String[] info = data.split(":");

						String key = info[1].trim();

						String originalSender = info[3];
						String[] selArgs = new String[]{originalSender};

						contentProvider.query(gUri,null,key,selArgs,null);

						outStream.writeBytes("queryAck\n");

					}
					if(data.startsWith("keyFound")){

						String[] info = data.split(":");

						String key = info[1].trim();
						String value = info[2].trim();
						bq.put(key+"-"+value);

						outStream.writeBytes("keyFoundAck\n");
					}
					if(data.startsWith("star")){

						Cursor cursor = contentProvider.query(gUri,null,"@",null,null);

						String out = "";
						if (cursor.moveToFirst()){
							do{
								String k1 = cursor.getString(cursor.getColumnIndex("key"));
								String v1 = cursor.getString(cursor.getColumnIndex("value"));
								out+=k1+"-"+v1+":";

								// do what ever you want here
							}while(cursor.moveToNext());
						}
						cursor.close();

						if(out.isEmpty()){
							out="*";
						}
						//Log.d(TAG,"RetrievedData::"+sender+":::"+out);

						outStream.writeBytes("starAck::"+out+ "\n");
					}

					//clientSocket.close();

				}

			} catch (Exception e) {


			}

		  return null;
		}
	}

	public ArrayList<String> getPrefList(String key){

		String coordNode="";
		ArrayList<String> repList = new ArrayList<String>();
		String keyHash="";
		try {
			keyHash = genHash(key);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		for(String k:nodeTable.keySet()){
			Log.d(TAG,"Checking Node: "+k);
			Node n = nodeTable.get(k);

			Log.d(TAG,"Comparing "+keyHash+": "+n.getPredId()+" :"+n.getNodeId());
			if( (keyHash.compareTo(n.getPredId())>0 && keyHash.compareTo(n.getNodeId())<0)
					|| (n.getPredId().compareTo(n.getNodeId()) >0 && keyHash.compareTo(n.getPredId())>0 )
					|| (n.getPredId().compareTo(n.getNodeId()) >0 && keyHash.compareTo(n.getNodeId())<0 )) {


				Log.d(TAG,"Wtf This shouldnt happen");
				coordNode = k;
				break;
			}

		}

		repList.add(coordNode);
		String next = nodeTable.get(coordNode).getSucc();
		repList.add(next);
		String last = nodeTable.get(next).getSucc();
		repList.add(last);

		Log.d(TAG,"FinalRepList:"+repList);
		return repList;
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {




		@Override
		protected Void doInBackground(String... msgs) {
			String msgToSend = msgs[0] + "\n";

			//sendPkt("Testing","11108","TestAck","TestLog");
			if (msgToSend.startsWith("insert")) {
				try {

					Log.d(TAG,"EnteredInsert"+msgToSend);
					String[] info = msgToSend.split(":");

					String key = info[1].trim();

					Log.d(TAG,"InsertionKey: "+key);

					ArrayList<String> repNodes = getPrefList(key);

					Log.d(TAG,"RepNodes: "+repNodes.toString());
					for (String node : repNodes) {

						String remotePort = Integer.toString(Integer.parseInt(node) * 2);
						Log.d(TAG,"Sending Insert "+key+": "+remotePort);
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(remotePort));



						DataOutputStream outStream = new DataOutputStream(socket.getOutputStream());

						outStream.writeBytes(msgToSend);

						BufferedReader in = new BufferedReader(
								new InputStreamReader(socket.getInputStream()));

						String ack = in.readLine();
						if (ack.startsWith("InsertAck")) {
							socket.close();
						}

					}


				}catch (Exception e){
					Log.d(TAG,"InsertionException: "+e);
				}

			}
			if(msgToSend.startsWith("query")){

				try {
					Log.d(TAG,"EnteredQuery"+msgToSend);
					String[] info = msgToSend.split(":");

					String key = info[1].trim();

					Log.d(TAG,"QueryingKey: "+key);
					String remotePort = Integer.toString(Integer.parseInt(info[2].trim()) * 2);
					Log.d(TAG,"Sending Query To"+key+": "+remotePort);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(remotePort));

					msgToSend = msgToSend.trim() + ":"+currNode+"\n";

					DataOutputStream outStream = new DataOutputStream(socket.getOutputStream());



					outStream.writeBytes(msgToSend);

					BufferedReader in = new BufferedReader(
								new InputStreamReader(socket.getInputStream()));

					String ack = in.readLine();
					if (ack.startsWith("queryAck")) {
						socket.close();
					}

				}catch (Exception e){
					Log.d(TAG,"QueryingException: "+e);
				}

			}
			if(msgToSend.startsWith("keyFound")){


				try {
					Log.d(TAG,"EnteredKeyFound"+msgToSend);
					String[] info = msgToSend.split(":");

					//String key = info[1].trim();

					//Log.d(TAG,"QueryingKey: "+key);
					String remotePort = Integer.toString(Integer.parseInt(info[3].trim()) * 2);
					//Log.d(TAG,"Sending Query To"+key+": "+remotePort);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(remotePort));

					//msgToSend = msgToSend.trim() + ":"+currNode+"\n";

					DataOutputStream outStream = new DataOutputStream(socket.getOutputStream());



					outStream.writeBytes(msgToSend);

					BufferedReader in = new BufferedReader(
							new InputStreamReader(socket.getInputStream()));

					String ack = in.readLine();
					if (ack.startsWith("keyFoundAck")) {
						socket.close();
					}

				}catch (Exception e){
					Log.d(TAG,"QueryingException: "+e);
				}

			}
			if(msgToSend.startsWith("starQuery")){

				String allVals="";
				for(String node:nodeTable.keySet()){
					if(node.equals(currNode))
						continue;

					try {
						Log.d(TAG,"EnteredStarQuery: "+msgToSend);


						//String key = info[1].trim();

						//Log.d(TAG,"QueryingKey: "+key);
						String remotePort = Integer.toString(Integer.parseInt(node) * 2);
						//Log.d(TAG,"Sending Query To"+key+": "+remotePort);
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(remotePort));

						//msgToSend = msgToSend.trim() + ":"+currNode+"\n";

						DataOutputStream outStream = new DataOutputStream(socket.getOutputStream());
						outStream.writeBytes(msgToSend);

						BufferedReader in = new BufferedReader(
								new InputStreamReader(socket.getInputStream()));

						String ack = in.readLine();
						if (ack.startsWith("starAck")) {

							String[] inf = ack.split("::");

							if(!inf[1].trim().equals("*"))
								allVals += inf[1].trim();

							socket.close();

						}


					}catch (Exception e){
						Log.d(TAG,"StarQueryingException: "+e);
					}
				}

				try {
					bq.put(allVals);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}

			return null;
		}
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

		Log.d(TAG,"Entered Delete: "+selection);

		if(selection.equals("*") || selection.equals("@")) {
			File[] files = getContext().getFilesDir().listFiles();
			for (File file : files) {

				file.delete();

			}

			/*if(selection.equals("*") && !predNode.isEmpty()){
				String deleteMsg = "Delete:"+succNode;
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,deleteMsg , cNode);
			}*/
		}
		else {

			File dir = getContext().getFilesDir();
			File file = new File(dir,selection);
			if(file.exists()) {
				file.delete();
				//Log.d(TAG,"Deleting "+selection+" at: "+cNode);
			}
			else {
				//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DelKey:" + selection, cNode);

				Log.d(TAG,"NeedToRouteDelete");
				//Log.d(TAG,"Couldnt delete: "+selection+ "passing to "+ succNode);
			}
		}

		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

		String key = values.getAsString("key");
		String value = values.getAsString("value");

		FileOutputStream outputStream;


		if(insertFlagMap.containsKey(key+":"+value)) {
			try {
				//outputStream = openFileOutput(key, Context.MODE_PRIVATE);
				outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
				outputStream.write(value.getBytes());
				outputStream.close();

			} catch (Exception e) {
				Log.e("GroupMessenger", "File write failed");
			}
			insertFlagMap.remove(key+":"+value);
		}
		else{
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"inserting:"+key+":"+value);

		}

		return uri;
	}


	public String fetchQueryVal(String selection){
		String value = "";
		try {
			StringBuilder builder = new StringBuilder();
			FileInputStream inputStream = getContext().openFileInput(selection);
			int ch;
			while ((ch = inputStream.read()) != -1) {
				builder.append((char) ch);
			}
			value = builder.toString();
			inputStream.close();

		} catch (Exception e) {
			Log.e(TAG, "ExceptionFile:" + e);
		}

		return value;

	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub

		String columnNames[] = {"key","value"};
		MatrixCursor cursor = new MatrixCursor(columnNames,1);



		if(selection.equals("*") || selection.equals("@")) {

			File[] files = getContext().getFilesDir().listFiles();
			for (File file : files) {

				String filename = file.toString();
				String pattern = Pattern.quote(System.getProperty("file.separator"));
				String[] splittedFileName = filename.split(pattern);

				String s = file.toString();
				cursor = fetchValue(cursor, splittedFileName[splittedFileName.length - 1]);
				Log.d(TAG, "Logging files: " + splittedFileName[splittedFileName.length - 1]);
			}

			if(selection.equals("*")){
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"starQuery");


				try {
					String starData = "";
					starData = bq.take();

					if(!starData.isEmpty()) {
						String[] kvPairs = starData.split(":");

						for (String kv : kvPairs) {
							Log.d(TAG, "KVPair:" + kv);
							String[] kvOut = kv.split("-");

							cursor.addRow(new Object[]{kvOut[0], kvOut[1]});

						}
					}



				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}

		}
		else if(selectionArgs==null || selectionArgs.length==0){

			ArrayList<String> contNodes = getPrefList(selection);

			if(contNodes.contains(currNode)) {

				String value = fetchQueryVal(selection);
				cursor.addRow(new Object[]{selection, value});
			}
			else{


				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"querying:"+selection+":"+contNodes.get(0));

				String keyVal = "";
				try {
					keyVal = bq.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				String[] kvPair = keyVal.split("-");

				String key = kvPair[0];
				String value = kvPair[1];
				cursor.addRow(new Object[]{key,value});



			}
		}
		else if(selectionArgs.length>0){
			String value = fetchQueryVal(selection);

			String originalSender = selectionArgs[0];
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"keyFound:"+selection+":"+value+":"+originalSender);

		}
		return cursor;
	}

	public MatrixCursor fetchValue(MatrixCursor cursor,String selection){


		String value="";
		try {
			StringBuilder builder = new StringBuilder();
			FileInputStream inputStream = getContext().openFileInput(selection);
			int ch;
			while ((ch = inputStream.read()) != -1) {
				builder.append((char) ch);
			}
			value = builder.toString();
			inputStream.close();
		} catch (FileNotFoundException e) {
			Log.e("GroupMessenger", "Unable to read file");
		} catch (IOException e) {
			e.printStackTrace();
		}
		cursor.addRow(new Object[]{selection, value});
		return cursor;
	}
	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	private class Node {
		//private Node succ = null;
		//private Node pred = null;
		private String currId;
		//private String node_id;
        private String succId;
		private String predId;
		private String succ;
		private String pred;

		public Node(String curr, String succ, String pred) {

			try{
			this.currId = genHash(curr);
			//this.node_id = node_id;
			this.succ = succ;
			this.pred = pred;

		    this.succId = genHash(succ);
			this.predId = genHash(pred);

			}catch (Exception e){

			}
		}

		public String getPred() {
			return this.pred;
		}

		public String getSucc() {
			return this.succ;
		}

		public String getNodeId() { return this.currId;}

		public String getSuccId() {
			return this.succId;
		}

		public String getPredId() {
			return this.predId;
		}

	}
}
