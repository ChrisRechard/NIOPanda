package MainTest;

import Connect.BroadcastState;
import Connect.ConnectDanMuServer;

import Statistic.RealtimeSta;
import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.Statement;
import org.json.JSONException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * 直播状态监听进程
 */
class broadcastListen extends Thread{

    private boolean isBroadcastStart = false;
    private int getCount = 0;
    private int getEndCount = 5;
    private int startTimesCount = 0;
    private String roomID;
    private volatile boolean isThreadStop = false;
    private fetchInfo proFetch;
    private DanmuStatistic DanmuThread;
    private BambooStatistic BambooRecThread;
    private MaobiStatistic MaobiThread;
    private VisitorStatistic VisitorThread;
    private FansNumStatistic FansSumThread;
    private BambooSumStatistic BambooSumThread;
//    private Thread Fetch;
//    private Thread Danmu;
//    private Thread Bamboo;
//    private Thread Maobi;
//    private Thread Visitor;
//    private Thread FansSum;
//    private Thread BambooSum;
    public broadcastListen(String rstring)
    {
        this.roomID=rstring;
    }
    //直播状态监听进程关闭方法
    public void closeThread(){
        getEndCount=0;
        DanmuThread.close();
        isThreadStop=true;
        proFetch.close();
        BambooRecThread.close();
        MaobiThread.close();
        VisitorThread.close();
        FansSumThread.close();
        BambooSumThread.close();
    }
    public void run(){

        while (!isThreadStop)
        {
            System.out.println(roomID+"Whole Process Start Start Start Start Start");
            //每次抓取时 初始化getcount
            getCount=0;
            getEndCount=3;
            //直播开始次数计数
            String driver = "com.mysql.jdbc.Driver";
            String url = "jdbc:mysql://5716e40e38f53.gz.cdb.myqcloud.com:10823/panda?useSSL=true";
            String user = "cdb_outerroot";
            String password = "fmm529529529";
            try {
                Class.forName(driver);
                Connection conn = DriverManager.getConnection(url, user, password);
                Statement statement = (Statement) conn.createStatement();
                ResultSet startcount= statement.executeQuery("select startcount from broadcaststart where"+" roomid = "+"\""+roomID+"\""+" AND broadcastdate = curdate()");
                if(!startcount.next()){
                    startTimesCount=0;
                }else {
                    startcount.last();
                    startTimesCount=startcount.getInt("startcount");
                }
                statement.close();
                conn.close();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            startTimesCount++;
            while (!isBroadcastStart)
            {
                try {
                    //等待直播开始
                    //System.out.println(roomID);
                    getCount = BroadcastState.getState(roomID,getCount,startTimesCount);
                    if (getCount != 0){
                        isBroadcastStart = true;
                        //直播开始 开始抓取信息
                        ConnectDanMuServer fetch = new ConnectDanMuServer();
                        proFetch =new fetchInfo(roomID,fetch);
//                        Fetch=new Thread(proFetch);
//                        Fetch.setDaemon(true);
//                        Fetch.start();
                        proFetch.start();
                        //开始统计弹幕
                        DanmuThread =new DanmuStatistic(roomID,startTimesCount);
//                        Danmu=new Thread(DanmuThread);
//                        Danmu.setDaemon(true);
//                        Danmu.start();
                        DanmuThread.start();
                        //开始统计收到的竹子
                        BambooRecThread =new BambooStatistic(roomID,startTimesCount);
//                        Bamboo=new Thread(BambooRecThread);
//                        Bamboo.setDaemon(true);
//                        Bamboo.start();
                        BambooRecThread.start();
                        //开始统计收到的猫币
                        MaobiThread = new MaobiStatistic(roomID,startTimesCount);
//                        Maobi=new Thread(MaobiThread);
//                        Maobi.setDaemon(true);
//                        Maobi.start();
                        MaobiThread.start();
                        //开始获取实时人气
                        VisitorThread = new VisitorStatistic(roomID);
//                        Visitor=new Thread(VisitorThread);
//                        Visitor.setDaemon(true);
//                        Visitor.start();
                        VisitorThread.start();
                        //开始获取实时关注人数
                        FansSumThread = new FansNumStatistic(roomID);
//                        FansSum=new Thread(FansSumThread);
//                        FansSum.setDaemon(true);
//                        FansSum.start();
                        FansSumThread.start();
                        //开始获取实时竹子总数
                        BambooSumThread = new BambooSumStatistic(roomID);
//                        BambooSum=new Thread(BambooSumThread);
//                        BambooSum.setDaemon(true);
//                        BambooSum.start();
                        BambooSumThread.start();
                    }

                } catch (JSONException e) {
                    e.printStackTrace();
                }
                //if (getCount ==0){
                    try {
                        Thread.currentThread().sleep((long)280000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                //}

            }
            if(isBroadcastStart)
            {
                while (getEndCount !=0) {
                    try {
                        //直播开始后  等待直播结束
                        getEndCount = BroadcastState.getState(roomID, getEndCount,startTimesCount);
                    } catch (JSONException e) {
                        e.printStackTrace();
                    }
                    if (getEndCount != 0){
                        try {
                            Thread.sleep((long)280000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                }
                if(getEndCount == 0)
                {
                    isBroadcastStart=false;
                    //直播结束 关闭信息抓取线程
                    //fetchInfo.close();
                    proFetch.close();
                    //关闭人气获取线程
                    VisitorThread.close();
                    //VisitorStatistic.close();
                    //关闭关注人数获取线程
                    FansSumThread.close();
                    //FansNumStatistic.close();
                    //关闭竹子总数获取线程
                    BambooSumThread.close();
                    //BambooSumStatistic.close();
                    //关闭弹幕统计线程
                    DanmuThread.close();
                    //DanmuStatistic.close();
                    //关闭竹子统计线程
                    BambooRecThread.close();
                    //BambooStatistic.close();
                    //关闭猫币统计线程
                    MaobiThread.close();
                    //MaobiStatistic.close();
                    try {
//                        FansSum.join();
//                        BambooSum.join();
//                        Fetch.join();
//                        Visitor.join();
//                        Danmu.join();
//                        Bamboo.join();
//                        Maobi.join();
                        FansSumThread.join();
                        BambooSumThread.join();
                        proFetch.join();
                        VisitorThread.join();
                        DanmuThread.join();
                        BambooRecThread.join();
                        MaobiThread.join();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                }

            }
        }
        System.out.println(roomID+"STOP STOP this Time Stop This Time");

    }

}

/**
 * 直播信息抓取线程
 */

class fetchInfo extends Thread{
    private String roomID;
    private ConnectDanMuServer connn;
    public fetchInfo(String roomstring,ConnectDanMuServer co){
        this.connn=co;
        this.roomID=roomstring;
    }
    public void close(){
        this.connn.Close();
        System.out.println("Fetinfo Close");
    }
    public void run(){
        System.out.println("FetchInfo Start Start Start");
        if(!connn.ConnectToDanMuServer(roomID)){
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            connn.ConnectToDanMuServer(roomID);
        }
    }
}

/**
 * 弹幕统计线程
 */
class DanmuStatistic extends Thread{
    private String roomID;
    private int stacounts;
    private volatile boolean isDanmuStatisticStop=false;
    public void close(){
        isDanmuStatisticStop=true;
    }
    public DanmuStatistic(String roomString, int stacount){
        this.roomID=roomString;
        this.stacounts=stacount;
    }
    public void run(){
        //弹幕统计线程开始时统计弹幕初始化
        System.out.println(roomID+"DanmuSta Start Start Start");
        int danmustatistic =0;
        while(!isDanmuStatisticStop){
            danmustatistic++;
            try {
                Thread.currentThread().sleep((long)183*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            RealtimeSta.danmuSta(roomID,stacounts,danmustatistic);
        }
        System.out.println(roomID+"DanmuSta Close");
    }
}

/**
 * 实时收到的竹子统计线程
 */
class BambooStatistic extends Thread{
    private String roomID;
    private int starcounts;
    private volatile boolean isBambooStatisticStop = false;
    public void close(){
        isBambooStatisticStop = true;
    }
    public BambooStatistic(String room, int starcount){
        this.roomID = room;
        this.starcounts = starcount;
    }
    public void run(){
        System.out.println(roomID+"BambooSta Start Start Start");
        int bamboostatistic = 0;
        while (!isBambooStatisticStop){
            bamboostatistic++;
            try {
                Thread.currentThread().sleep((long)183*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            RealtimeSta.BambooRecSta(roomID,starcounts,bamboostatistic);
        }
        System.out.println(roomID+"BambooSta Close");
    }

}

/**
 * 实时收到的猫币统计线程
 */
class MaobiStatistic extends Thread{
    private String roomID;
    private int starcounts;
    private volatile boolean isMaobiStatisticStop = false;
    public void close(){
        isMaobiStatisticStop = true;
    }
    public MaobiStatistic(String room, int starcount){
        this.roomID = room;
        this.starcounts = starcount;
    }
    public void run() {
        System.out.println(roomID+"MaobiSta Start Start Start");
        int maobistatistic = 0;
        while (!isMaobiStatisticStop) {
            maobistatistic++;
            try {
                Thread.currentThread().sleep((long) 183 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            RealtimeSta.PresentRecSta(roomID, starcounts, maobistatistic);
        }
        System.out.println(roomID+"MaobiSta Close");
    }
}
/**
 * 实时人气获取线程，每两分钟获取一次
 */
class VisitorStatistic extends Thread{
    private String roomID;
    private boolean isVisitorStatisticStop = false;
    public void close(){
        isVisitorStatisticStop = true;
    }
    public VisitorStatistic(String room){
        this.roomID = room;
    }
    public void run() {
        System.out.println(roomID+"VisitorSta Start Start Start");
        while (!isVisitorStatisticStop) {
            try {
                RealtimeSta.VisitorSta(roomID);
            } catch (JSONException e) {
                e.printStackTrace();
            }
            try {
                Thread.currentThread().sleep((long) 200 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println(roomID+"VisitorSta Close");
    }
}

/**
 * 直播关注人数实时获取，每五分钟获取一次
 */
class FansNumStatistic extends Thread{
    private String roomID;
    private volatile boolean isFansNumStatisticStop = false;
    public void close(){
        isFansNumStatisticStop = true;
    }
    public FansNumStatistic(String room){
        this.roomID = room;
    }
    public void run() {
        System.out.println(roomID+"FansNumSta Start Start Start");
        while (!isFansNumStatisticStop) {
            try {
                RealtimeSta.FansNumSta(roomID);
            } catch (JSONException e) {
                e.printStackTrace();
            }
            try {
                Thread.sleep((long) 300 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println(roomID+"FansNumSta Close");
    }

}

/**
 * 直播竹子总数获取，每2五分钟获取一次
 */
class BambooSumStatistic extends Thread{
    private String roomID;
    private volatile boolean isBambooSumStatisticStop = false;
    public void close(){
        isBambooSumStatisticStop = true;
    }
    public BambooSumStatistic(String room){
        this.roomID = room;
    }
    public void run() {
        while (!isBambooSumStatisticStop) {
            try {
                RealtimeSta.BambooSumSta(roomID);
            } catch (JSONException e) {
                e.printStackTrace();
            }
            try {
                Thread.currentThread().sleep((long) 300 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println(roomID+"BambooSumSta Close");
    }
}
public class TestDemo{

    public static void main(String[] args){
        //String[] roomlist = {"324101","322655","335019","319751","323346","322652","332495","315168","349196","312273","268792","324033","319709","316223"};
        //String[] roomlist = {"324101"};
        //String[] roomlist = {"322655"};
        //String[] roomlist = {"335019"};
        //String[] roomlist = {"319751"};
        //String[] roomlist = {"323346"};

        //String[] roomlist = {"322652"};
        //String[] roomlist = {"332495"};
        //String[] roomlist = {"315168"};
        //String[] roomlist = {"319709"};
        //String[] roomlist = {"316223"};

        //String[] roomlist = {"349196"};
        //String[] roomlist = {"312273"};
        //String[] roomlist = {"268792"};
        //String[] roomlist = {"324033"};
        //String[] roomlist = {"324103"};

        //String[] roomlist = {"324101","322655","335019","319751","323346"};
        //String[] roomlist = {"322652","332495","315168","319709","316223"};
        String[] roomlist={"349196","312273","268792","324033","324103"};
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://5716e40e38f53.gz.cdb.myqcloud.com:10823/panda?useSSL=true";
        String user = "cdb_outerroot";
        String password = "fmm529529529";
        Connection conn=null;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, user, password);
            if (!conn.isClosed())
                System.out.println("Succeeded connecting to the Database!");
            Statement statement = (Statement) conn.createStatement();
            //存储弹幕信息的表
            //String sql2 = "create table IF NOT EXISTS danmuinfo (roomId integer,recTime TIMESTAMP,danmu text)";
            String sql1 = "create table IF NOT EXISTS danmuinfo (roomid integer,recTime TIMESTAMP)";
            String sql2 = "create table IF NOT EXISTS zhuziinfo (roomid integer,recTime TIMESTAMP,zhuzi integer)";
            String sql3 = "create table IF NOT EXISTS presentinfo (roomid integer,recTime TIMESTAMP,presentvalue INTEGER)";
            String sql4 = "create table IF NOT EXISTS broadcaststart (roomid integer,broadcastdate text,starttime TIMESTAMP,startcount INTEGER)";
            String sql5 = "create table IF NOT EXISTS vistorSta (roomid integer,recTime TIMESTAMP,recdate text,audienceNum integer)";
            String sql6 = "create table IF NOT EXISTS danmuSta (roomid integer,periodstart TIMESTAMP,recdate text,danmunum INTEGER)";
            String sql7 = "create table IF NOT EXISTS bambooSta (roomid integer,start TIMESTAMP,recdate text,bamboorec INTEGER)";
            String sql8 = "create table IF NOT EXISTS maobiSta (roomid integer,start TIMESTAMP,recdate text,maobirec INTEGER)";
            String sql9 = "create table IF NOT EXISTS fansnumSta (roomid integer,curtime TIMESTAMP,recdate text,curfansnum INTEGER)";
            String sql10 = "create table IF NOT EXISTS bambooSumSta (roomid integer,curtime TIMESTAMP,recdate text,bamboosum INTEGER)";
            //String sql11 = "create table IF NOT EXISTS roomcurrent (roomid integer,roomstate INTEGER)";
            int r1 = statement.executeUpdate(sql1);
            int r2 = statement.executeUpdate(sql2);
            int r3 = statement.executeUpdate(sql3);
            int r4 = statement.executeUpdate(sql4);
            int r5 = statement.executeUpdate(sql5);
            int r6 = statement.executeUpdate(sql6);
            int r7 = statement.executeUpdate(sql7);
            int r8 = statement.executeUpdate(sql8);
            int r9 = statement.executeUpdate(sql9);
            int r10 = statement.executeUpdate(sql10);
            //int r11 = statement.executeUpdate(sql11);
            if (r1 == -1) {
                System.out.println("Create danmuInfo Fails");
            }
            if (r2 == -1) {
                System.out.println("Create zhuziInfo Fails");
            }
            if (r3 == -1) {
                System.out.println("Create persentInfo Fails");
            }
            if (r4 == -1) {
                System.out.println("Create startInfo Fails");
            }
            if (r5 == -1) {
                System.out.println("Create VisitorSta Fails");
            }
            if (r6 == -1) {
                System.out.println("Create DanmuSta Fails");
            }
            if (r7 == -1) {
                System.out.println("Create BambooSta Fails");
            }
            if (r8 == -1) {
                System.out.println("Create maobiSta Fails");
            }
            if (r9 == -1) {
                System.out.println("Create fansnumSta Fails");
            }
            if (r10 == -1) {
                System.out.println("Create BambooSUM SUM SUM Sta Fails");
            }
//            statement.close();
//            conn.close();
        } catch (Exception e) {
            // TODO: handle exception
            System.out.println("Sorry,can`t find the Driver!");
            e.printStackTrace();
        }finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        for(int i=0;i<roomlist.length;i++)
        {
            broadcastListen PL=new broadcastListen(roomlist[i]);
            PL.start();
        }
    }
}