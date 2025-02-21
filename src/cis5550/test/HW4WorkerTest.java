package cis5550.test;

import java.util.*;
import java.io.*;
import java.net.*;
import java.util.concurrent.*;

public class HW4WorkerTest extends GenericTest {

  String id;

  HW4WorkerTest() {
    super();
    id = null;
  }

  void runSetup2() throws Exception {
    File f = new File("__worker");
    if (!f.exists())
      f.mkdir();

    File f2 = new File("__worker"+File.separator+"id");
    if (f2.exists())
      f2.delete();

    id = null;
  }

  void runSetup1() throws Exception {
    File f = new File("__worker");
    if (!f.exists())
      f.mkdir();

    PrintWriter idOut = new PrintWriter("__worker"+File.separator+"id");
    id = randomAlphaNum(5,5);
    idOut.print(id);
    idOut.close();
  }

  void cleanup() throws Exception {
    File f2 = new File("__worker"+File.separator+"id");
    if (f2.exists())
      f2.delete();

    File f = new File("__worker");
    if (f.exists())
      f.delete();
  }

  void prompt() {
    /* Ask the user to confirm that the server is running */

    File f = new File("__worker");
    System.out.println("In two separate terminal windows, please run:");
    System.out.println("* java cis5550.kvs.Coordinator 8000");
    System.out.println("* java cis5550.kvs.Worker 8001 "+f.getAbsolutePath()+" localhost:8000");
    System.out.println("and then hit Enter in this window to continue. If the Coordinator and/or the Worker are already running, please terminate them and restart the test suite!");
    (new Scanner(System.in)).nextLine();
  }

  void runTests(Set<String> tests) throws Exception {
    System.out.printf("\n%-10s%-40sResult\n", "Test", "Description");
    System.out.println("--------------------------------------------------------");

    if (tests.contains("cond-put1")) try {
        startTest("cond-put1", "ec1", 10);
        Socket s = openSocket(8001);
        PrintWriter out = new PrintWriter(s.getOutputStream());
        String cell = "/data/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8);
        String data = randomAlphaNum(10,20);
        String req = "PUT "+cell;
        out.print(req+" HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+data.length()+"\r\n\r\n"+data);
        out.flush();
        Response r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
        if (!r.body().equals("OK"))
          testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return 'OK', and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
        s.close();

        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        req = "GET "+cell;
        out.print(req+" HTTP/1.1\r\nHost: localhost\r\n\r\n");
        out.flush();
        r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
        if (!r.body().equals(data))
          testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return the string we had PUT in earlier ("+data+"), and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
        s.close();

        testSucceeded();
      } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }
    
    if (tests.contains("cond-put2")) try {
        startTest("cond-put2", "ec1", 10);
        Socket s = openSocket(8001);
        PrintWriter out = new PrintWriter(s.getOutputStream());
        String cell = "/data/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8);
        String data = randomAlphaNum(10,20);
        String req = "PUT "+cell +"?ifcolumn=condcolumn";
        out.print(req+" HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+data.length()+"\r\n\r\n"+data);
        out.flush();
        Response r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
        if (!r.body().equals("OK"))
          testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return 'OK', and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
        s.close();

        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        req = "GET "+cell;
        out.print(req+" HTTP/1.1\r\nHost: localhost\r\n\r\n");
        out.flush();
        r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
        if (!r.body().equals(data))
          testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return the string we had PUT in earlier ("+data+"), and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
        s.close();

        testSucceeded();
      } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }
    
    if (tests.contains("cond-put3")) try {
        startTest("cond-put3", "ec1", 10);
        Socket s = openSocket(8001);
        PrintWriter out = new PrintWriter(s.getOutputStream());
        String cell = "/data/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8);
        String data = randomAlphaNum(10,20);
        String req = "PUT "+cell +"?equals=data";
        out.print(req+" HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+data.length()+"\r\n\r\n"+data);
        out.flush();
        Response r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
        if (!r.body().equals("OK"))
          testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return 'OK', and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
        s.close();

        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        req = "GET "+cell;
        out.print(req+" HTTP/1.1\r\nHost: localhost\r\n\r\n");
        out.flush();
        r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
        if (!r.body().equals(data))
          testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return the string we had PUT in earlier ("+data+"), and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
        s.close();

        testSucceeded();
      } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }
    
    if (tests.contains("cond-put4")) try {
        startTest("cond-put4", "ec1", 10);
        Socket s = openSocket(8001);
        PrintWriter out = new PrintWriter(s.getOutputStream());
        String cell = "/data/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8);
        String data = randomAlphaNum(10,20);
        String req = "PUT "+cell +"?ifcolumn=condcolumn&equals=true";
        out.print(req+" HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+data.length()+"\r\n\r\n"+data);
        out.flush();
        Response r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
        if (!r.body().equals("OK"))
          testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return 'OK', and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
        s.close();

        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        req = "GET "+cell;
        out.print(req+" HTTP/1.1\r\nHost: localhost\r\n\r\n");
        out.flush();
        r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
        if (!r.body().equals(data))
          testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return the string we had PUT in earlier ("+data+"), and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
        s.close();

        testSucceeded();
      } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }
    
    if (tests.contains("cond-put5")) try {
        startTest("cond-put5", "ec1", 10);
        Socket s = openSocket(8001);
        PrintWriter out = new PrintWriter(s.getOutputStream());
        String table = randomAlphaNum(5,8);
        String row = randomAlphaNum(5,8);
        String column = randomAlphaNum(5,8);
        
        String initialize = "/data/"+table+"/"+row+"/condcolumn";
        String initializeData = "True";
        String initializeReq = "PUT "+initialize;
        out.print(initializeReq+" HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+initializeData.length()+"\r\n\r\n"+initializeData);
        out.flush();
        Response r = readAndCheckResponse(s, "response");
        s.close();
        
        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        String condput = "/data/"+table+"/"+row+"/"+column;
        String data = randomAlphaNum(10, 20);
        String req = "PUT "+condput +"?ifcolumn=condcolumn&equals=True";
        out.print(req+" HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+data.length()+"\r\n\r\n"+data);
        out.flush();
        r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
        if (!r.body().equals("OK"))
          testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return 'OK', and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
        s.close();

        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        req = "GET "+ condput;
        out.print(req+" HTTP/1.1\r\nHost: localhost\r\n\r\n");
        out.flush();
        r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
        if (!r.body().equals(data))
          testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return the string we had PUT in earlier ("+data+"), and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
        s.close();

        testSucceeded();
      } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }
    
    if (tests.contains("cond-put6")) try {
        startTest("cond-put6", "ec1", 10);
        Socket s = openSocket(8001);
        PrintWriter out = new PrintWriter(s.getOutputStream());
        String table = randomAlphaNum(5,8);
        String row = randomAlphaNum(5,8);
        String column = randomAlphaNum(5,8);
        
        String initialize = "/data/"+table+"/"+row+"/condcolumn";
        String initializeData = "True";
        String initializeReq = "PUT "+initialize;
        out.print(initializeReq+" HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+initializeData.length()+"\r\n\r\n"+initializeData);
        out.flush();
        Response r = readAndCheckResponse(s, "response");
        s.close();
        
        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        String condput = "/data/"+table+"/"+row+"/"+column;
        String data = randomAlphaNum(10, 20);
        String req = "PUT "+condput +"?ifcolumn=condcolumn&equals=test";
        out.print(req+" HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+data.length()+"\r\n\r\n"+data);
        out.flush();
        r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
        if (!r.body().equals("FAIL"))
          testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return 'FAIL', and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
        s.close();

        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        req = "GET "+ condput;
        out.print(req+" HTTP/1.1\r\nHost: localhost\r\n\r\n");
        out.flush();
        r = readAndCheckResponse(s, "response");
        if (r.statusCode != 404)
          testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 404 Not Found. Here is what was in the body:\n\n"+dump(r.body));
        s.close();

        testSucceeded();
      } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }
    
    if (tests.contains("ver-1")) try {
        startTest("ver-1", "ec2", 10);
        Socket s = openSocket(8001);
        PrintWriter out = new PrintWriter(s.getOutputStream());
        String cell = "/data/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8);
        String data = randomAlphaNum(10,20);
        String req = "PUT "+cell;
        out.print(req+" HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+data.length()+"\r\n\r\n"+data);
        out.flush();
        Response r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
        if (!r.body().equals("OK"))
          testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return 'OK', and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
        s.close();

        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        req = "GET "+cell;
        out.print(req+" HTTP/1.1\r\nHost: localhost\r\n\r\n");
        out.flush();
        r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
        if (!r.body().equals(data))
          testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return the string we had PUT in earlier ("+data+"), and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
        s.close();

        testSucceeded();
      } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }
    
    if (tests.contains("ver-2")) try {
        startTest("ver-2", "ec2", 10);
        Socket s = openSocket(8001);
        PrintWriter out = new PrintWriter(s.getOutputStream());
        String cell1 = "/data/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8);
        String data1 = randomAlphaNum(10,20);
        String req1 = "PUT "+cell1;
        out.print(req1+" HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+data1.length()+"\r\n\r\n"+data1);
        out.flush();
        Response r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a "+r.statusCode+" response to our "+req1+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
        if (!r.body().equals("OK"))
          testFailed("The server did return a 200 status code to our "+req1+", but it was supposed to return 'OK', and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
        s.close();
        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        String cell2 = "/data/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8);
        String data2 = randomAlphaNum(10,20);
        String req2 = "PUT "+cell2;
        out.print(req2+" HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+data2.length()+"\r\n\r\n"+data2);
        out.flush();
        r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a "+r.statusCode+" response to our "+req2+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
        if (!r.body().equals("OK"))
          testFailed("The server did return a 200 status code to our "+req2+", but it was supposed to return 'OK', and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
        s.close();

        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        req1 = "GET "+cell1;
        out.print(req1+" HTTP/1.1\r\nHost: localhost\r\n\r\n");
        out.flush();
        r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a "+r.statusCode+" response to our "+req1+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
        if (!r.body().equals(data1))
          testFailed("The server did return a 200 status code to our "+req1+", but it was supposed to return the string we had PUT in earlier ("+data1+"), and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
        s.close();
        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        req2 = "GET "+cell2;
        out.print(req2+" HTTP/1.1\r\nHost: localhost\r\n\r\n");
        out.flush();
        r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a "+r.statusCode+" response to our "+req2+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
        if (!r.body().equals(data2))
          testFailed("The server did return a 200 status code to our "+req2+", but it was supposed to return the string we had PUT in earlier ("+data2+"), and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
        s.close();

        testSucceeded();
      } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }
    
    if (tests.contains("ver-3")) try {
        startTest("ver-3", "ec2", 10);
        Socket s = openSocket(8001);
        PrintWriter out = new PrintWriter(s.getOutputStream());
        String cell = "/data/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8);
        String data = randomAlphaNum(10,20);
        String req = "PUT "+cell;
        out.print(req+" HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+data.length()+"\r\n\r\n"+data);
        out.flush();
        Response r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
        if (!r.body().equals("OK"))
          testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return 'OK', and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
        s.close();

        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        req = "GET "+cell;
        out.print(req+" HTTP/1.1\r\nHost: localhost\r\n\r\n");
        out.flush();
        r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
        if (!r.body().equals(data))
          testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return the string we had PUT in earlier ("+data+"), and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
        s.close();

        testSucceeded();
      } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }
    
    if (tests.contains("ver-4")) try {
        startTest("ver-4", "ec2", 10);
        Socket s = openSocket(8001);
        PrintWriter out = new PrintWriter(s.getOutputStream());
        String cell = "/data/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8);
        String data = randomAlphaNum(10,20);
        String req = "PUT "+cell;
        out.print(req+" HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+data.length()+"\r\n\r\n"+data);
        out.flush();
        Response r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
        if (!r.body().equals("OK"))
          testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return 'OK', and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
        s.close();

        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        req = "GET "+cell;
        out.print(req+" HTTP/1.1\r\nHost: localhost\r\n\r\n");
        out.flush();
        r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
        if (!r.body().equals(data))
          testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return the string we had PUT in earlier ("+data+"), and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
        s.close();

        testSucceeded();
      } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }
    
    if (tests.contains("ver-5")) try {
        startTest("ver-5", "ec2", 10);
        Socket s = openSocket(8001);
        PrintWriter out = new PrintWriter(s.getOutputStream());
        String cell = "/data/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8);
        String data = randomAlphaNum(10,20);
        String req = "PUT "+cell;
        out.print(req+" HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+data.length()+"\r\n\r\n"+data);
        out.flush();
        Response r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
        if (!r.body().equals("OK"))
          testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return 'OK', and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
        s.close();

        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        req = "GET "+cell;
        out.print(req+" HTTP/1.1\r\nHost: localhost\r\n\r\n");
        out.flush();
        r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
        if (!r.body().equals(data))
          testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return the string we had PUT in earlier ("+data+"), and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
        s.close();

        testSucceeded();
      } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }
    
    if (tests.contains("ver-6")) try {
        startTest("ver-6", "ec2", 10);
        Socket s = openSocket(8001);
        PrintWriter out = new PrintWriter(s.getOutputStream());
        String cell = "/data/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8);
        String data = randomAlphaNum(10,20);
        String req = "PUT "+cell;
        out.print(req+" HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+data.length()+"\r\n\r\n"+data);
        out.flush();
        Response r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
        if (!r.body().equals("OK"))
          testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return 'OK', and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
        s.close();

        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        req = "GET "+cell;
        out.print(req+" HTTP/1.1\r\nHost: localhost\r\n\r\n");
        out.flush();
        r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
          testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
        if (!r.body().equals(data))
          testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return the string we had PUT in earlier ("+data+"), and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
        s.close();

        testSucceeded();
      } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }
    
    if (tests.contains("ver-7")) try {
        startTest("ver-7", "ec2 massive test", 10);
        String tableID = randomAlphaNum(5,8);
        String rowID = randomAlphaNum(5,8);
        
        Socket s = openSocket(8001);
        PrintWriter out = new PrintWriter(s.getOutputStream());
        String req1 = "PUT /data/"+tableID+"/"+rowID+"/a";
        String data1 = "true";
        out.print(req1+" HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+data1.length()+"\r\n\r\n"+data1);
        out.flush();
        Response r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
            testFailed("Needed 200 response 1");
        if (!r.body().equals("OK"))
            testFailed("bad body on req1");
        s.close();
        
        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        String req2 = "PUT /data/"+tableID+"/"+rowID+"/b";
        String data2 = "10";
        out.print(req2+" HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+data2.length()+"\r\n\r\n"+data2);
        out.flush();
        Response r2 = readAndCheckResponse(s, "response");
        if (r2.statusCode != 200)
            testFailed("Needed 200 response 2");
        if (!r2.body().equals("OK"))
            testFailed("bad body on req1");
        s.close();
        
        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        String req3 = "PUT /data/"+tableID+"/"+rowID+"/c";
        String data3 = "hi";
        out.print(req3+" HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+data3.length()+"\r\n\r\n"+data3);
        out.flush();
        Response r3 = readAndCheckResponse(s, "response");
        if (r3.statusCode != 200)
            testFailed("Needed 200 response 3");
        if (!r3.body().equals("OK"))
            testFailed("bad body on req1");
        s.close();
        
        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        String req4 = "PUT /data/"+tableID+"/"+rowID+"/a";
        String data4 = "false";
        out.print(req4+" HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+data4.length()+"\r\n\r\n"+data4);
        out.flush();
        Response r4 = readAndCheckResponse(s, "response");
        if (r4.statusCode != 200)
            testFailed("Needed 200 response 4");
        if (!r4.body().equals("OK"))
            testFailed("bad body on req1");
        s.close();
        
        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        String req5 = "PUT /data/"+tableID+"/"+rowID+"/a";
        String data5 = "maybe";
        out.print(req5+" HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+data5.length()+"\r\n\r\n"+data5);
        out.flush();
        Response r5 = readAndCheckResponse(s, "response");
        if (r5.statusCode != 200)
            testFailed("Needed 200 response 5");
        if (!r5.body().equals("OK"))
            testFailed("bad body on req1");
        s.close();

            

        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        String getreq1 = "GET /data/"+tableID+"/"+rowID+"/a";
        out.print(getreq1+" HTTP/1.1\r\nHost: localhost\r\n\r\n");
        out.flush();
        r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
            testFailed("Needed 200 response 6");
        if (!r.body().equals(data5))
            testFailed("bad body on get1");
        s.close();
        
        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        String getreq7 = "GET /data/"+tableID+"/"+rowID+"/b";
        out.print(getreq7+" HTTP/1.1\r\nHost: localhost\r\n\r\n");
        out.flush();
        r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
            testFailed("Needed 200 response 7");
        if (!r.body().equals(data2))
            testFailed("bad body on getreq7");
        s.close();
        
        
        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        String getreq13 = "GET /data/"+tableID+"/"+rowID+"/c";
        out.print(getreq13+" HTTP/1.1\r\nHost: localhost\r\n\r\n");
        out.flush();
        r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
            testFailed("Needed 200 response 8");
        if (!r.body().equals(data3))
            testFailed("bad body on getreq13");
        s.close();
        
        
        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        String req6 = "PUT /data/"+tableID+"/"+rowID+"/b?ifcolumn=a&equals=maybe";
        String data6 = "25";
        out.print(req6+" HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+data6.length()+"\r\n\r\n"+data6);
        out.flush();
        Response r6 = readAndCheckResponse(s, "response");
        if (r6.statusCode != 200)
            testFailed("Needed 200 response 9");
        if (!r6.body().equals("OK"))
            testFailed("bad body on req6");
        s.close();
        
        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        String getreq16 = "GET /data/"+tableID+"/"+rowID+"/a";
        out.print(getreq16+" HTTP/1.1\r\nHost: localhost\r\n\r\n");
        out.flush();
        r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
            testFailed("Needed 200 response 10");
        if (!r.body().equals(data5))
            testFailed("bad body on getreq16");
        s.close();
        
        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        String getreq17 = "GET /data/"+tableID+"/"+rowID+"/b";
        out.print(getreq17+" HTTP/1.1\r\nHost: localhost\r\n\r\n");
        out.flush();
        r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
            testFailed("Needed 200 response 11");
        if (!r.body().equals(data6))
            testFailed("bad body on getreq17");
        s.close();
        
        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        String getreq18 = "GET /data/"+tableID+"/"+rowID+"/c";
        out.print(getreq18+" HTTP/1.1\r\nHost: localhost\r\n\r\n");
        out.flush();
        r = readAndCheckResponse(s, "response");
        if (r.statusCode != 200)
            testFailed("Needed 200 response 12");
        if (!r.body().equals(data3))
            testFailed("bad body on getreq18");
        s.close();
        
        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        String getreq19 = "GET /data/"+tableID+"/"+rowID+"/d";
        out.print(getreq19+" HTTP/1.1\r\nHost: localhost\r\n\r\n");
        out.flush();
        r = readAndCheckResponse(s, "response");
        if (r.statusCode != 404)
            testFailed("Needed 404 response 13");
        if (r.headers.containsKey("version")) {
            testFailed("Should not contain version header");
        }
        s.close();
        
        s = openSocket(8001);
        out = new PrintWriter(s.getOutputStream());
        String getreq20 = "GET /data/"+tableID+"/"+rowID+"/e";
        out.print(getreq20+" HTTP/1.1\r\nHost: localhost\r\n\r\n");
        out.flush();
        r = readAndCheckResponse(s, "response");
        if (r.statusCode != 404)
            testFailed("Needed 404 response 14");
        if (r.headers.containsKey("version")) {
            testFailed("Should not contain version header");
        }
        s.close();
        
        

        testSucceeded();
      } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }
    
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    if (tests.contains("read-id")) try {
      setTimeoutMillis(30000);
      startTest("read-id", "Read the worker's ID (takes 20 seconds)", 5);
      Thread.sleep(20000);
      File f = new File("__worker");
      if (id == null) 
        id = (new Scanner(new File("__worker"+File.separator+"id"))).nextLine();

      Socket s = openSocket(8000);
      PrintWriter out = new PrintWriter(s.getOutputStream());
      out.print("GET /workers HTTP/1.1\r\nHost: localhost\r\n\r\n");
      out.flush();
      Response r = readAndCheckResponse(s, "response");
      if (r.statusCode != 200)
        testFailed("The coordinator returned a "+r.statusCode+" response to our GET /workers, but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
      String[] pieces = r.body().split("\n");
      if (!pieces[0].equals("1"))
        testFailed("The coordinator did return a 200 status code to our GET /workers, but it was supposed to return a '1' in the first line, and it didn't. Here is what was in the body:\n\n"+dump(r.body)+"\nMaybe the worker didn't register properly? Check the ping thread!");
      if ((pieces.length < 2) || !pieces[1].contains(id))
        testFailed("The coordinator did return one worker in its response to our GET /workers, but the ID we had written to __worker/id ("+id+") did not appear. Here is what the coordinator sent:\n\n"+dump(r.body)+"\nMaybe the worker didn't read the ID frome the 'id' file in the storage directory - or maybe you didn't provide the correct path when you started the worker? It was supposed to be "+f.getAbsolutePath()+". Also, remember to start the worker AFTER the test suite (when you are prompted to hit Enter); if it is already running, the test suite will overwrite the ID and then expect to see the new value, but the worker won't see it.");
      s.close();

      testSucceeded();
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    setTimeoutMillis(-1);

    if (tests.contains("put")) try {
      startTest("put", "Individual PUT", 5);
      Socket s = openSocket(8001);
      PrintWriter out = new PrintWriter(s.getOutputStream());
      String data = randomAlphaNum(200,500);
      String req = "PUT /data/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8);
      out.print(req+" HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+data.length()+"\r\n\r\n"+data);
      out.flush();
      Response r = readAndCheckResponse(s, "response");
      if (r.statusCode != 200)
        testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
      if (!r.body().equals("OK"))
        testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return 'OK', and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
      s.close();
      testSucceeded();
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("putget")) try {
      startTest("putget", "PUT a value and then GET it back", 10);
      Socket s = openSocket(8001);
      PrintWriter out = new PrintWriter(s.getOutputStream());
      String cell = "/data/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8);
      String data = randomAlphaNum(10,20);
      String req = "PUT "+cell;
      out.print(req+" HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+data.length()+"\r\n\r\n"+data);
      out.flush();
      Response r = readAndCheckResponse(s, "response");
      if (r.statusCode != 200)
        testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
      if (!r.body().equals("OK"))
        testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return 'OK', and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
      s.close();

      s = openSocket(8001);
      out = new PrintWriter(s.getOutputStream());
      req = "GET "+cell;
      out.print(req+" HTTP/1.1\r\nHost: localhost\r\n\r\n");
      out.flush();
      r = readAndCheckResponse(s, "response");
      if (r.statusCode != 200)
        testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
      if (!r.body().equals(data))
        testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return the string we had PUT in earlier ("+data+"), and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
      s.close();

      testSucceeded();
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    if (tests.contains("overwrite")) try {
      startTest("overwrite", "Overwrite a value", 5);
      Socket s = openSocket(8001);
      PrintWriter out = new PrintWriter(s.getOutputStream());
      String cell = "/data/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8)+"/"+randomAlphaNum(5,8);
      String data1 = randomAlphaNum(10,20);
      String req = "PUT "+cell;
      out.print(req+" HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+data1.length()+"\r\n\r\n"+data1);
      out.flush();
      Response r = readAndCheckResponse(s, "response");
      if (r.statusCode != 200)
        testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
      if (!r.body().equals("OK"))
        testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return 'OK', and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
      s.close();

      String data2 = data1;
      while (data2.equals(data1))
        data2 = randomAlphaNum(10,20);

      s = openSocket(8001);
      out = new PrintWriter(s.getOutputStream());
      req = "PUT "+cell;
      out.print(req+" HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+data2.length()+"\r\n\r\n"+data2);
      out.flush();
      r = readAndCheckResponse(s, "response");
      if (r.statusCode != 200)
        testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
      if (!r.body().equals("OK"))
        testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return 'OK', and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
      s.close();

      s = openSocket(8001);
      out = new PrintWriter(s.getOutputStream());
      req = "GET "+cell;
      out.print(req+" HTTP/1.1\r\nHost: localhost\r\n\r\n");
      out.flush();
      r = readAndCheckResponse(s, "response");
      if (r.statusCode != 200)
        testFailed("The server returned a "+r.statusCode+" response to our "+req+", but we were expecting a 200 OK. Here is what was in the body:\n\n"+dump(r.body));
      if (r.body().equals(data1))
        testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return the new value we had PUT ("+data2+"), and it returned the old value ("+data1+") instead.");
      if (!r.body().equals(data2))
        testFailed("The server did return a 200 status code to our "+req+", but it was supposed to return the second string we had PUT in ("+data2+"), and it didn't. Here is what was in the body instead:\n\n"+dump(r.body));
      s.close();

      testSucceeded();
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    boolean stressRan = false;
    long reqPerSec = 0;
    long numFail = 0;
    if (tests.contains("stress")) try {
      setTimeoutMillis(45000);
      startTest("stress", "Stress test with 25,000 requests", 5);
      final ConcurrentHashMap<String,Integer> ver = new ConcurrentHashMap<String,Integer>();
      final ConcurrentHashMap<String,Integer> lck = new ConcurrentHashMap<String,Integer>();
      final ConcurrentHashMap<String,String> dat = new ConcurrentHashMap<String,String>();
      final int numThreads = 30;
      final int testsTotal = 25000;
      final Counter testsRemaining = new Counter(testsTotal);
      final Counter testsFailed = new Counter(0);
      final String tabname = randomAlphaNum(10,20);
      Thread t[] = new Thread[numThreads];
      long tbegin = System.currentTimeMillis();
      for (int i=0; i<numThreads; i++) {
        t[i] = new Thread("Client thread "+i) { public void run() {
          while (testsRemaining.aboveZero()) {
            try {
              Socket s = openSocket(8001);
              PrintWriter out = new PrintWriter(s.getOutputStream());
              while (testsRemaining.decrement()) {
                String row = randomAlphaNum(1,1);
                if (random(0,1) == 0) {
                  int expectedAtLeastVersion = 0;
                  synchronized(ver) {
                    if (ver.containsKey(row))
                      expectedAtLeastVersion = ver.get(row).intValue();
                  }
                  out.print("GET /data/"+tabname+"/"+row+"/col HTTP/1.1\r\nHost: localhost\r\n\r\n");
                  out.flush();
                  Response r = readAndCheckResponse(s, "response");
                  if (expectedAtLeastVersion > 0) {
                    if (r.statusCode != 200) {
                      testsFailed.increment();
                      System.err.println("GET returned code "+r.statusCode+" for row: "+row+", body: "+new String(r.body));
                    } else {
                      String val = new String(r.body);
                      String[] pcs = val.split("-");
                      int v = Integer.valueOf(pcs[0]).intValue();
                      if (v < expectedAtLeastVersion) {
                        testsFailed.increment();
                        System.err.println("GET returned version "+v+" for row "+row+", but that has been overwritten; we expected at least version "+expectedAtLeastVersion);
                      } if (!dat.get(row+"-"+v).equals(val)) {
                        testsFailed.increment();
                        System.err.println("GET returned value "+val+" for row "+row+", but we expected version "+v+" to be "+dat.get(row+"-"+v).equals(val));
                      }
                    }
                  }
                } else {
                  String val;
                  int nextVersion = 0;
                  synchronized(ver) {
                    while (lck.containsKey(row))
                      row = randomAlphaNum(1,1);
                    lck.put(row, Integer.valueOf(1));
                    if (ver.containsKey(row))
                      nextVersion = ver.get(row).intValue() + 1;
                    val = nextVersion+"-"+randomAlphaNum(10,20);
                    dat.put(row+"-"+nextVersion, val);
                  }
                  out.print("PUT /data/"+tabname+"/"+row+"/col HTTP/1.1\r\nHost: localhost\r\nContent-Length: "+val.length()+"\r\n\r\n"+val);
                  out.flush();
                  Response r = readAndCheckResponse(s, "response");
                  if (r.statusCode != 200) {
                    testsFailed.increment();
                    System.err.println("PUT returned code "+r.statusCode+" for row: "+row+", body: "+new String(r.body));
                  }
                  synchronized(ver) {
                    ver.put(row, nextVersion);
                    lck.remove(row);
                  }
                }
              }
              s.close();
            } catch (Exception e) { e.printStackTrace(); }
          }
        } };
        t[i].start();
      }
      for (int i=0; i<numThreads; i++)
        t[i].join();
      long tend = System.currentTimeMillis();

      stressRan = true;
      numFail = testsFailed.getValue();
      reqPerSec = 1000*testsTotal/(tend-tbegin);

      if (numFail>0)
        testFailed("Looks like "+numFail+" of the "+testsTotal+" requests failed. Check your code for concurrency issues! (This is a very difficult test case, so you may want to leave it until the very end, when your implementation passes all the other tests.)");
      if (reqPerSec<1000)
        testFailed("Looks like your solution handled "+reqPerSec+" requests/second; it should be at least 1,000. Try disabling any debug output?  (This is a very difficult test case, so you may want to leave it until the very end, when your implementation passes all the other tests.)");

      testSucceeded();
    } catch (Exception e) { testFailed("An exception occurred: "+e, false); e.printStackTrace(); }

    setTimeoutMillis(-1);


    System.out.println("--------------------------------------------------------\n");
    if (numTestsFailed == 0)
      System.out.println("Looks like your solution passed all of the selected tests. Congratulations!");
    else
      System.out.println(numTestsFailed+" test(s) failed.");
    if (stressRan) 
      System.out.println("\nYour throughput in the stress test was "+reqPerSec+" requests/sec, with "+numFail+" failues");
    cleanup();
    closeOutputFile();
  }

	public static void main(String args[]) throws Exception {

    /* Make a set of enabled tests. If no command-line arguments were specified, run all tests. */

    Set<String> tests = new TreeSet<String>();
    boolean runSetup1 = true, runSetup2 = false, runTests = true, promptUser = true, outputToFile = false, exitUponFailure = true, cleanup = false;

    if ((args.length > 0) && (args[0].equals("auto1") || args[0].equals("auto2"))) {
      runSetup1 = false;
      runSetup2 = false;
      runTests = true;
      outputToFile = true;
      exitUponFailure = false;
      promptUser = false;
      cleanup = false;
    } else if ((args.length > 0) && (args[0].equals("setup1") || args[0].equals("setup2"))) {
      runSetup1 = args[0].equals("setup1");
      runSetup2 = args[0].equals("setup2");
      runTests = false;
      promptUser = false;
      cleanup = false;
    } else if ((args.length > 0) && (args[0].equals("cleanup1") || (args[0].equals("cleanup2")))) {
      runSetup1 = false;
      runSetup2 = false;
      runTests = false;
      promptUser = false;
      cleanup = true;
    } else if ((args.length > 0) && args[0].equals("version")) {
      System.out.println("HW4 worker autograder v1.3 (Sep 19, 2023)");
      System.exit(1);
    }

    if ((args.length == 0) || args[0].equals("all") || args[0].equals("auto1")) {
      // EC1 tests
        //tests.add("cond-put1");
        //tests.add("cond-put2");
        //tests.add("cond-put3");
        //tests.add("cond-put4");
        //tests.add("cond-put5");
        //tests.add("cond-put6");
      
        // EC2 tests
        tests.add("ver-1");
        tests.add("ver-2");
        tests.add("ver-3");
        tests.add("ver-4");
        tests.add("ver-5");
        tests.add("ver-6");
        tests.add("ver-7");
        
        
        
      tests.add("read-id");
      tests.add("put");
      tests.add("putget");
      tests.add("overwrite");
      tests.add("stress");
    } else if ((args.length > 0) && args[0].equals("auto2")) {
      tests.add("newid");
      tests.add("writeid");
    } 

    for (int i=0; i<args.length; i++)
      if (!args[i].equals("all") && !args[i].equals("auto1") && !args[i].equals("auto2") && !args[i].equals("setup1") && !args[i].equals("setup2") && !args[i].equals("cleanup1") && !args[i].equals("cleanup2"))
        tests.add(args[i]);

    HW4WorkerTest t = new HW4WorkerTest();
    t.setExitUponFailure(exitUponFailure);
    if (outputToFile)
      t.outputToFile();
    if (runSetup1)
      t.runSetup1();
    if (runSetup2)
      t.runSetup2();
    if (promptUser)
      t.prompt();
    if (runTests)
      t.runTests(tests);
    if (cleanup)
      t.cleanup();
  }
} 
