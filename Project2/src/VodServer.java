
import java.net.*; 
import java.io.*; 
import java.lang.Thread;
import java.util.*;
import java.text.SimpleDateFormat;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.UUID;
import java.util.Properties;


class EchoHandler extends Thread{
    // extend Thread for handling all clients' connection
    Socket clientSocket;
    DatagramSocket udpSocket;
    String contentPath;
    String name;
    String uuid;
    int peerNumber;
    List<String> peers;
    String confFilePath;
    static final int BUFFERSIZE = 1024;
    private static final int DATA_SIZE = 1024;
    EchoHandler (Socket clientSocket, DatagramSocket udpSocket, String uuid, String name, String contentPath,
                 int peerNumber, String confFilePath, List<String> peers){
        this.clientSocket=clientSocket;
        this.udpSocket = udpSocket;
        this.uuid = uuid;
        this.name = name;
        this.peerNumber = peerNumber;
        this.contentPath = contentPath;
        this.confFilePath = confFilePath;
        this.peers = peers;
    }

    public void run() {
        try{
            System.out.println ("Connection successful");
            System.out.println ("Waiting for input.....");
            //PrintWriter out = new PrintWriter(clientSocket.getOutputStream(),true); 
            OutputStream output = clientSocket.getOutputStream();
            //PrintWriter out = new PrintWriter(output);
            BufferedReader in = new BufferedReader( 
                new InputStreamReader( clientSocket.getInputStream())); 
            String inputLine;
            // String Range = null;
            //inputLine = in.readLine();
            // read the request input line
            while ((inputLine = in.readLine()) != null) {
            //while (true) {
                String Range = null;
                //inputLine = in.readLine();
                System.out.println(inputLine);
                //System.out.println(inputLine.length());
                if (inputLine.length() <= 0) {  
                    break;  
                } 
                String[] request = inputLine.split(" ");
                String method = request[0];
                String url = request[1]; 
                // System.out.println("start:");
                // System.out.println(method);
                // System.out.println(url);
                // System.out.println("End");
                String host = "";
                String port = "";
                String rate = "";
                String path = "";
                
                // Parse peer url
                if(url.startsWith("/peer/add")&&!url.startsWith("/peer/addneighbor")){
                    StringBuilder sb = new StringBuilder();
                    String[]  urlcomp = url.split("&");
                    String[] pathcomp = urlcomp[0].split("=");
                    //path = pathcomp[1];
                    //System.out.println(path);
                    sb.append(pathcomp[1]).append(",");
                    String[] hostcomp = urlcomp[1].split("=");
                    //host += hostcomp[1];
                    //System.out.println(host);
                    sb.append(hostcomp[1]).append(",");
                    String[] portcomp = urlcomp[2].split("=");
                    //port += portcomp[1];
                    //System.out.println(port);
                    sb.append(portcomp[1]).append(",");
                    if(urlcomp.length > 3){
                        String[] ratecomp = urlcomp[3].split("=");
                        //rate += ratecomp[1];
                        sb.append(ratecomp[1]);
                    }
                    else{
                        //rate += "0";
                        sb.append(0);
                    }
                    try{
                        File peers = new File("peers.txt");
                        FileOutputStream fos = null;
                        if(!peers.exists()){
                            peers.createNewFile();
                            fos = new FileOutputStream(peers);
                        }else{
                            fos = new FileOutputStream(peers, true);
                        }
                        OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
                        osw.write(sb.toString());
                        osw.write("\r\n");
                        osw.flush();
                        osw.close();
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
                else if(url.startsWith("/peer/view")){
                    String targetContent = url.replace("/peer/view/","");
                    //System.out.println("target content is" + targetContent);
                    try{
                        File readIn = new File("peers.txt");
                        BufferedReader br = new BufferedReader(new FileReader(readIn));
                        String para;
                        while((para = br.readLine())!=null){
                            //System.out.println(para);
                            if(para.startsWith(targetContent)) {
                                //System.out.println(para);
                                String[] paraComp = para.split(",");
                                path = paraComp[0];
                                host = paraComp[1];
                                port = paraComp[2];
                                rate = paraComp[3];
                                break;
                            }
                        }
                        System.out.println("path: "+path+" host: "+host+" port: "+port+" rate: "+rate);
                        InetAddress address = InetAddress.getByName(host);
                        byte[] syndata = "SYN".getBytes();
                        System.out.println(syndata.length);
                        //int syn_size = syndata.length();
                        byte[] firstdata = new byte[DATA_SIZE+3];
                        System.arraycopy(syndata, 0, firstdata, 0, 3);
                
                        DatagramPacket firstPacket = new DatagramPacket(firstdata, firstdata.length, address, Integer.parseInt(port));
                        udpSocket.send(firstPacket);
                        //udpnodeE node1 = new udpnodeE(Integer.parseInt(port), address);//port应该是自己的
                        //node1.start();
                        br.close();
                        File file = new File("received_fileE.txt");
                        res200OK(file, output);
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                }
                else if(url.startsWith("/peer/kill")){
                    System.out.println("Process terminated by user.");
                    System.exit(0);
                }
                else if(url.startsWith("/peer/uuid")){
                    String jsonString = "{\"uuid\":" + "\""+uuid+"\"}";
                    BufferedWriter bw = new BufferedWriter(new FileWriter("./json/uuid.json"));
                    bw.write(jsonString);
                    bw.close();
                    res200OK(new File("./json/uuid.json"), output);
                }
                else if(url.startsWith("/peer/addneighbor")){
                    String peerinfo = url.replace("/peer/addneighbor?","");
                    peers.add(peerinfo);

                    ++peerNumber;
                    File confFile = new File(confFilePath);
                    try{
                        FileReader reader = new FileReader(confFile);
                        Properties props = new Properties();
                        props.load(reader);

                        props.setProperty("peer_"+ (peerNumber-1), peerinfo);
                        props.setProperty("peer_count", String.valueOf(peerNumber));
                        FileWriter writer = new FileWriter(confFile);
                        props.store(writer, "Add new peer");
                        writer.close();
                    }
                    catch (FileNotFoundException e){
                        e.printStackTrace();
                    }
                }

                path = "./"+ contentPath + url.substring(0);
                //String path = url;
                //System.out.println(path);
                
                // read the method and check if it is "GET"
                if (method.equals("GET")) {
                    File file = new File(path);
                    System.out.println(file);
                    if (!file.exists()) {
                        //System.out.println("404");
                        res404NotFound(output);
                    } else {
                        // read the request inputline and check if there is Range
                        while(inputLine != null) {
                            //System.out.println(inputLine);
                            if (inputLine.startsWith("Range: ")) {
                                Range = inputLine.substring(7);
                                //break;
                            }
                            inputLine = in.readLine();
                            if (inputLine.length() <= 0) {  
                                break;  
                            } 
                        }
                        if(Range != null){
                            System.out.println(Range);
                            res206PartialContent(file,output,Range);
                        } else {
                            //System.out.println("200");
                            res200OK(file, output);
                        }
                    }
                } else {
                    // unexpected request
                    res405MethodNotAllowed(output);
                    res500InternalServerError(output);
                }
            
            }
            //System.out.println("out while");
            clientSocket.close();
        }
        catch (Exception e){
            //System.err.println(e);
            //res500InternalServerError();
            StringWriter stringWriter = new StringWriter();
            e.printStackTrace(new PrintWriter(stringWriter));
            String error = stringWriter.toString();
            //System.out.println(error);
            if(!error.startsWith("java.net.SocketException")){
             //&& !error.startsWith("java.net.SocketException: Connection reset by peer: socket write error")){
                System.err.println("Exception caught: Client Disconnected.");
            }
            
        }
        finally{
            try{
                clientSocket.close();
            }
            catch(Exception e){;}
        }
    }
    private static void res404NotFound(OutputStream output) throws IOException {
        // send 404 response for not found file request
        PrintWriter out = new PrintWriter(output);
        final String CRLF = "\r\n";
        String pattern = "EEE, dd MMM yyyy HH:mm:ss z";
        SimpleDateFormat sdf = new SimpleDateFormat(pattern,Locale.ENGLISH);
        TimeZone timeZone = TimeZone.getTimeZone("GMT");
        sdf.setTimeZone(timeZone);
        Date nowDate = new Date(); 
        String nowDateGMT = sdf.format(nowDate);
        System.out.println(nowDateGMT);
        
        String response = "HTTP/1.1 404 Not Found" + CRLF +
                "Date: " + nowDateGMT + CRLF +
                "Connection: " + "Keep-Alive" + CRLF +
                CRLF;
        out.println(response);
        output.write(response.getBytes("UTF-8"));
        out.flush();
    }
    private static void res405MethodNotAllowed(OutputStream output) throws IOException {
        // send 405 response for unexpected request methods
        PrintWriter out= new PrintWriter(output);
        final String CRLF = "\r\n";
        String pattern = "EEE, dd MMM yyyy HH:mm:ss z";
        SimpleDateFormat sdf = new SimpleDateFormat(pattern,Locale.ENGLISH);
        TimeZone timeZone = TimeZone.getTimeZone("GMT");
        sdf.setTimeZone(timeZone);
        Date nowDate = new Date(); 
        String nowDateGMT = sdf.format(nowDate);
        System.out.println(nowDateGMT);
        String response = "HTTP/1.1 405 Method Not Allowed" + CRLF +
                "Date: " + nowDateGMT + CRLF +
                "Connection: " + "Keep-Alive" + CRLF +
                CRLF;
        out.println(response);
        output.write(response.getBytes("UTF-8"));
        out.flush();
    }

    private static void res500InternalServerError(OutputStream output) throws IOException {
        // send 500 response for server error
        PrintWriter out = new PrintWriter(output);
        final String CRLF = "\r\n";
        
        String response = "HTTP/1.1 500 Internal Server Error" + CRLF +
                "Content-Type: text/plain" + CRLF +
                "Content-Length: 19" + CRLF +
                CRLF;
        out.println(response);
        output.write(response.getBytes("UTF-8"));
        out.flush();
    }
    private static void res200OK(File file, OutputStream output) throws IOException {
        // send 200 response with GMT Zone formatted time
        String contentType = getContentType(file);
        PrintWriter out = new PrintWriter(output);
        final String CRLF = "\r\n";
        String pattern = "EEE, dd MMM yyyy HH:mm:ss z";
        SimpleDateFormat sdf = new SimpleDateFormat(pattern,Locale.ENGLISH);
        TimeZone timeZone = TimeZone.getTimeZone("GMT");
        sdf.setTimeZone(timeZone);
        Date nowDate = new Date(); 
        String nowDateGMT = sdf.format(nowDate);
        System.out.println(nowDateGMT);
        
        String response = "HTTP/1.1 200 OK" + CRLF +
                "Date: " + nowDateGMT + CRLF +
                "Connection: " + "Keep-Alive" + CRLF +
                "Content-Length: " + file.length() + CRLF +
                "Content-Type: " + contentType + CRLF +
                "Last-Modified" + LastModifiedDate(file) + CRLF +
                CRLF;
        out.println(response);
        output.write(response.getBytes("UTF-8"));
        // send the whole file
        FileInputStream fileStream = new FileInputStream(file);
        byte[] buffer = new byte[BUFFERSIZE];
        int part;
        while ((part = fileStream.read(buffer)) >= 0) {
            output.write(buffer, 0, part);
        }
        fileStream.close();
        out.flush();
    }
    private static void res206PartialContent(File file, OutputStream output, String Range) throws IOException {
        // split the range and get the left and right value, and send 206 response
        String contentType = getContentType(file);
        String[] partContent = Range.split("=")[1].split("-");
        int length = partContent.length;
        long left;
        long right;
        System.out.println(partContent.length);
        final String CRLF = "\r\n";
        String pattern = "EEE, dd MMM yyyy HH:mm:ss z";
        SimpleDateFormat sdf = new SimpleDateFormat(pattern,Locale.ENGLISH);
        TimeZone timeZone = TimeZone.getTimeZone("GMT");
        sdf.setTimeZone(timeZone);
        Date nowDate = new Date(); 
        String nowDateGMT = sdf.format(nowDate);
        System.out.println(nowDateGMT);
        String response;
        PrintWriter out = new PrintWriter(output);
        if(length == 1){
            left = Long.parseLong(partContent[0]);
            right = file.length() - 1;
            long contentLength = right - left + 1;
            response = "HTTP/1.1 206 Partial Content" + CRLF +
            "Date: " + nowDateGMT + CRLF +
            "Connection: " + "Keep-Alive" + CRLF +
            "Content-Length: " + contentLength + CRLF +
            "Content-Range: bytes " + left + "-" + right  + "/" +file.length() + CRLF + 
            "Content-Type: " + contentType + CRLF +
            "Last-Modified" + LastModifiedDate(file) + CRLF +
            CRLF;
        }else {
            left = Long.parseLong(partContent[0]);
            right = Long.parseLong(partContent[1]);
            long contentLength = right - left + 1;
            response = "HTTP/1.1 206 Partial Content" + CRLF +
            "Date: " + nowDateGMT + CRLF +
            "Connection: " + "Keep-Alive" + CRLF +
            "Content-Length: " + contentLength + CRLF +
            "Content-Range: bytes " + left + "-" + right + "/" +file.length() + CRLF + 
            "Content-Type: " + contentType + CRLF +
            "Last-Modified" + LastModifiedDate(file) + CRLF +
            CRLF;
        }
        out.println(response);
        output.write(response.getBytes("UTF-8"));
        // send the partial content based on range request
        FileInputStream fileInput = new FileInputStream(file);
        fileInput.skip(left);
        byte[] buffer = new byte[BUFFERSIZE];
        int part;
        while ((part = fileInput.read(buffer)) >= 0 && left <= right) {
            output.write(buffer, 0, part);
            left += part;
        }
        fileInput.close();
        out.flush();
    }

    private static String getContentType(File file) {
        // check file type
        String content = file.getName();
        if (content.endsWith(".txt")) {
            return "text/plain";
        } else if (content.endsWith(".css")) {
            return "text/css";
        } else if (content.endsWith(".htm") || content.endsWith(".html")) {
            return "text/html";
        } else if (content.endsWith(".gif")) {
            return "image/gif";
        } else if (content.endsWith(".jpg") || content.endsWith(".jpeg")) {
            return "image/jpeg";
        } else if (content.endsWith(".png")) {
            return "image/png";
        } else if (content.endsWith(".js")) {
            return "application/javascript";
        } else if (content.endsWith(".json")){
            return "application/json";
        } else if (content.endsWith(".mp4") || content.endsWith(".webm") || content.endsWith(".ogg")) {
            return "video/webm";
        } else {
            return "application/octet-stream";
        }
    }

    private static String LastModifiedDate(File file) {
        // standard GMT Zone lastest modified date format
        String pattern = "EEE, dd MMM yyyy HH:mm:ss z";
        SimpleDateFormat sdf = new SimpleDateFormat(pattern,Locale.ENGLISH);
        TimeZone timeZone = TimeZone.getTimeZone("GMT");
        sdf.setTimeZone(timeZone);
        Long lastModifiedDate = file.lastModified();
        return sdf.format(lastModifiedDate);
    }

}

public class VodServer { 
    // private DatagramSocket udpSocket;
    // private int udpPort;
    // private InetAddress address;
    private static final int DATA_SIZE = 1024;

    // public VodServer(int udpPort, InetAddress address) throws IOException {
    //     this.udpPort = udpPort;
    //     this.address = address;
    //     this.udpSocket = new DatagramSocket(udpPort);
    // }

    public static class SenderThread implements Runnable {
        private DatagramSocket udpSocket;
        private InetAddress address;
        private int port;

        public SenderThread(DatagramSocket udpSocket, InetAddress address, int port) {
            this.udpSocket = udpSocket;
            this.address = address;
            this.port = port;
        }

        @Override
        public void run() {
            try {
                // Wait for SYN
                InetAddress urlAdd = InetAddress.getByName("localhost"); // a ip string for input of the class
                int urlPort = 7077; //distination
                // byte[] synData = new byte[1024];
                // DatagramPacket synPacket = new DatagramPacket(synData, synData.length);
                // System.out.println("Wait for SYN");
                // socket.receive(synPacket);
                // String syn = new String(synPacket.getData(), 0, synPacket.getLength());
                // System.out.println("F get syn: "+syn);
                // if (!syn.equals("SYN")) {
                //     System.out.println("SYN received");
                // }
               
                // Send the file
                System.out.println("waiting for file transfer request");
                String filepath = "./content" + "/1.txt";
                File file = new File(filepath);
                byte[] buffer = new byte[(int) file.length()];
                FileInputStream fis = new FileInputStream(file);
                fis.read(buffer, 0, buffer.length);
                fis.close();

                int packetSize = 1024;
                int numberOfPackets = (int) Math.ceil((double) buffer.length / packetSize);
                System.out.println("chunk numbers:");
                System.out.println(numberOfPackets);
                int lastPacketSize = buffer.length % packetSize;
                if (lastPacketSize == 0) {
                    lastPacketSize = packetSize;
                }

                byte[] data = new byte[packetSize];
                for (int i = 0; i < numberOfPackets; i++) {
                    if (i == numberOfPackets - 1) {
                        data = Arrays.copyOfRange(buffer, i * packetSize, i * packetSize + lastPacketSize);
                    } else {
                        data = Arrays.copyOfRange(buffer, i * packetSize, (i + 1) * packetSize);
                    }

                    DatagramPacket packet = new DatagramPacket(data, data.length, address, port);
                    udpSocket.send(packet);

                    // // Wait for ACK
                    // byte[] receiveData = new byte[DATA_SIZE+3];
                    // DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                    // System.out.println("Wait for ACK");
                    // socket.receive(receivePacket);
                    // //System.out.println("ACK received");

                    // String ack = new String(receivePacket.getData(), 0, 3);
                    // System.out.println(ack);
                    // if (!ack.equals("ACK")) {
                    //     System.err.println("Error: ACK not received.");
                    //     break;
                    // }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class ReceiverThread implements Runnable {
        private DatagramSocket udpSocket;
        public ReceiverThread(DatagramSocket udpSocket) {
            this.udpSocket = udpSocket;
        }

        @Override
        public void run() {
            try {
                System.out.println("waiting for URL parsing result");
                InetAddress urlAdd = InetAddress.getByName("localhost"); // a ip string for input of the class
                int urlPort = 7077; //distination
                // // Send SYN
                // byte[] syndata = "SYN".getBytes();
                // DatagramPacket synPacket = new DatagramPacket(syndata, syndata.length, urlAdd, urlPort);
                // socket.send(synPacket);
                while (true) {
                    // Receive the file
                    byte[] buffer = new byte[DATA_SIZE+3];
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    //System.out.println("waiting for data packet");
                    udpSocket.receive(packet);
                    System.out.println(packet.getAddress());
                    System.out.println(packet.getPort());
                    String tag = new String(packet.getData(), 0, 3);
                    System.out.println("receiver tag: "+tag);
                    if (tag.equals("SYN")) {
                        System.out.println("start a sender thread");
                        Thread newsender = new Thread(new SenderThread(udpSocket, packet.getAddress(), packet.getPort()));
                        newsender.start();
                    } else if(tag.equals("ACK")) {
                        System.out.println("GOT ack");
                    } else {
                        FileOutputStream fos = new FileOutputStream("received_fileE.txt");
                        fos.write(packet.getData(), 0, packet.getLength());

                        byte[] ack = "ACK".getBytes();
                        byte[] ackdata = new byte[DATA_SIZE+3];
                        System.arraycopy(ack, 0, ackdata, 0, 3);
                        DatagramPacket ackPacket = new DatagramPacket(ackdata, ackdata.length, packet.getAddress(), packet.getPort());
                        udpSocket.send(ackPacket);
                        fos.close();
                    }
                    // Write the received data to a file
                    //File file = new File("received_fileF.txt");
                    //java.nio.file.Files.write(file.toPath(), packet.getData());
                    // FileOutputStream fos = new FileOutputStream("received_fileF.txt");
                    // fos.write(packet.getData(), 0, packet.getLength());
                
                    // Send ACK
                    // byte[] data = "ACK".getBytes();
                    // DatagramPacket ackPacket = new DatagramPacket(data, data.length, packet.getAddress(), packet.getPort());
                    // socket.send(ackPacket);
                    // fos.close();
                    
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /// now to do
    public static void main(String[] args) throws IOException {
        ServerSocket serverSocket = null;
        String confFilePath = "";

        //fields for reading up config file
        String uuid = "";
        String name = "";
        String contentPath = "";
        int peerNumber = 0;
        int udpPort = 0;
        int PortNumber = 0;

        List<String> peers = new ArrayList<>();

        System.out.println(args[0]);
        System.out.println(args[1]);

        if (args.length == 2 && args[0].compareTo("-c") == 0) {
            confFilePath = args[1];
        }
        else if (args.length > 2) {
            System.out.println("Invalid file numbers, please provide only one file.");
            System.exit(1);
        }
        else{
            confFilePath = "node.conf";
        }

        File confFile = new File(confFilePath);

        try{
            FileReader reader = new FileReader(confFile);
            Properties props = new Properties();
            props.load(reader);

            uuid = props.getProperty("uuid");
            name = props.getProperty("name");
            contentPath = props.getProperty("content_dir");
            PortNumber = Integer.parseInt(props.getProperty("frontend_port"));
            udpPort = Integer.parseInt(props.getProperty("backend_port"));
            peerNumber = Integer.parseInt(props.getProperty("peer_count"));

            for(int i = 0; i < peerNumber; i++){
                String peerInstance = props.getProperty("peer_"+ i);
                peers.add(peerInstance);
            }

            reader.close();

            if(uuid.length()==0){
                uuid = UUID.randomUUID().toString();
                props.setProperty("uuid",uuid);
                FileWriter writer = new FileWriter(confFile);
                props.store(writer, "Assign new UUID");
                writer.close();
            }
        }
        catch (FileNotFoundException e){
            e.printStackTrace();
        }
        catch (IOException e){
            e.printStackTrace();
        }


        try {
            serverSocket = new ServerSocket(PortNumber);
            System.out.println("http port set");
        } catch (IOException e) {
            System.err.println("Could not listen on port you want to listen.");
            System.exit(1);
        }

        System.out.println("Waiting for connection.....");

        try {
            DatagramSocket udpSocket = new DatagramSocket(udpPort);
            System.out.println("udp port set");
            Thread receiver = new Thread(new ReceiverThread(udpSocket));
            receiver.start();
            while (true) {
                    Socket clientSocket = serverSocket.accept();
                    EchoHandler handler = new EchoHandler(clientSocket, udpSocket, uuid, name, contentPath, peerNumber,
                            confFilePath, peers);
                    handler.start();
            }
        } catch (IOException e) {
                System.err.println("Accept failed.");
                System.exit(1);
        }

        serverSocket.close();
    }
}