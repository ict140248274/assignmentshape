/**
 * Created by Rudy on 23/9/2016.
 */
import com.restfb.DefaultFacebookClient;
import com.restfb.FacebookClient;
import com.restfb.Version;
import org.apache.commons.lang3.ArrayUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.simple.JSONArray;

import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by TaiMing on 25/8/2016.
 */
public class test_get_json {
    private static String readAll(Reader rd) throws IOException {
        StringBuilder sb = new StringBuilder();
        int cp;
        while ((cp = rd.read()) != -1) {
            sb.append((char) cp);
        }
        return sb.toString();
    }

    public static JSONObject readJsonFromUrl(String url) throws IOException, JSONException {
        InputStream is = new URL(url).openStream();
        try {
            BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
            String jsonText = readAll(rd);
            JSONObject json = new JSONObject(jsonText);
            return json;
        } finally {
            is.close();
        }
    }
    public static void main(String [] args) {
        java.util.Timer timer = new java.util.Timer(false);
        java.util.TimerTask log_user = new java.util.TimerTask() {
            @Override
            public void run() {
                //db connect variable--------------------------------------------------------------------------------------------------------------------------------------------------------
                String sqlurl = "jdbc:mysql://103.254.208.224:3306/";
                String dbName = "advwhere_new";
                String driver = "com.mysql.jdbc.Driver";
                String userName = "advwhere";
                String password = "wnk6hED26mL4";
                //fb getting variable--------------------------------------------------------------------------------------------------------------------------------------------------
                long fbId = 0;
                long fans_count = 0;
                double[] followersgrowth = null;
                double[] postinteraction = null;
                String[][] top5mentions = null;
                String[][] top5tags = null;
                String[][] engagment = null;
                String db_bio = "";
                String export_json = "";
                String accessToken = "EAAB3p5B7z2cBAP1AJobxZBqAzx0eKzCepIP0Y7pSTb0rc028enygNtoSunuJ942FzjxZCj9wUHyi8xdYJ9a53sS8vFk0ZAkigjxv8UpBcwfE15scflfY57HHNZBpZAfdFuPy91ns4OwJhScd7lRAC";
                FacebookClient fbClient = new DefaultFacebookClient(accessToken, Version.VERSION_2_3);
                List<String> dblist = new ArrayList<String>();
                List<String> getupdatelist = new ArrayList<String>();
                List<String> getpostlist = new ArrayList<String>();
                try

                {
                    Class.forName(driver).newInstance();
                    java.sql.Connection conn = DriverManager.getConnection(sqlurl + dbName, userName, password);
                    Statement st = conn.createStatement();
                    ResultSet res = st.executeQuery("SELECT * FROM ig_user");
                    while (res.next()) {
                        String db_igId = res.getString("igId");
                        dblist.add(db_igId);
                    }

                } catch (
                        Exception e)

                {
                    e.printStackTrace();
                }

                long enddate = new java.util.Date().getTime();//--------lifetime
                long startdate = new java.util.Date().getTime() - (7 * 24 * 60 * 60 * 1000);//------oldtime
                long end90date = new java.util.Date().getTime();
                Calendar cal2 = Calendar.getInstance();
                cal2.add(Calendar.DATE, -97);
                long start90date = cal2.getTimeInMillis();
                Date date = new Date(enddate);
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                String pattern = "EEE";
                SimpleDateFormat format = new SimpleDateFormat(pattern, Locale.US);
                try

                {
                    for (int d = 0; d < dblist.size(); d++) {
                        System.out.println(startdate);
                        System.out.println(enddate);
                        System.out.println("followersgrowth");
                        followersgrowth = getFollowersGrowth(startdate, enddate, "" + dblist.get(d));
                        postinteraction = getPostInteraction(startdate, enddate, "" + dblist.get(d),start90date,end90date);
                        System.out.println("postinteraction");
                        System.out.println("getJSON");
                        System.out.println(exportJSON(followersgrowth, postinteraction, startdate, enddate));
                        String json = exportJSON(followersgrowth, postinteraction, startdate, enddate);
                        System.out.println(sendJSONtoDB(dblist.get(d), json));
                    }
                } catch (
                        Exception e)

                {
                    e.printStackTrace();
                }
        /*for(int i = 0; i< 1; i++){
            System.out.println("double"+followersgrowth[0]+","+followersgrowth[1]+","+followersgrowth[2]);
            System.out.println("double"+postinteraction[0]+","+postinteraction[1]+","+postinteraction[2]);
        }
        for(int j = 0; j<top5mentions.length; j++){
            System.out.println(top5mentions[j][0]+","+top5mentions[j][1]);
        }
        for(int j = 0; j<top5tags.length; j++){
            System.out.println(top5tags[j][0]+","+top5tags[j][1]);
        }*/
                long logDateTime3 = new java.util.Date().getTime();
                System.out.println("logTime is"+ logDateTime3);
            }
        };
        java.util.Date time = new java.util.Date();
        long delay = 86400000; //day = 86400000
        long period = 5000;

        //启动定时任务，立即执行壹次退出
        //timer.schedule(task, time);

        //启动定时任务，在 time 指定的时间执行壹次，然后每隔两秒执行壹次
        timer.schedule(log_user, time, delay);
    }
    public static String sendJSONtoDB(String igid,String json){
        String sqlurl = "jdbc:mysql://103.254.208.224:3306/";
        String dbName = "advwhere_new";
        String driver = "com.mysql.jdbc.Driver";
        String userName = "advwhere";
        String password = "wnk6hED26mL4";
        try{
            Class.forName(driver).newInstance();
            dbName = "advwhere_new";
            Connection conn3 = DriverManager.getConnection(sqlurl + dbName, userName, password);
            Statement st3 = conn3.createStatement();
            int val = st3.executeUpdate("UPDATE ig_user SET graphDataJSON = '"+json+"' where igid = '"+igid+"'");
            if(val == 1){
                System.out.println("send JSON successfully");
            }
            conn3.close();
        }catch(Exception e){
            e.printStackTrace();
        }
        return "finish send JSON process";
    }
    public static double[] getFollowersGrowth(long startdate, long enddate, String igid){
        String sqlurl = "jdbc:mysql://localhost:3306/";
        String dbName = "advwhere";
        String driver = "com.mysql.jdbc.Driver";
        String userName = "root";
        String password = "";
        double range = 0;
        double percentage = 0;
        long newdate = 0;
        List<Integer> logdatelist = new ArrayList<Integer>();
        List<Double> followersgrowthlist = new ArrayList<Double>();
        try {
            Class.forName(driver).newInstance();
            java.sql.Connection conn = DriverManager.getConnection(sqlurl+dbName,userName,password);
            Statement st = conn.createStatement();
            ResultSet res = st.executeQuery("SELECT * FROM ig_user_log where igId = '"+igid+"'AND logDateTime BETWEEN  '"+startdate+"'AND'"+enddate+"'");
            while (res.next()) {
                int db_updatedtime = res.getInt("followers");
                System.out.println("followers");
                logdatelist.add(db_updatedtime);
            }
            System.out.println("logdatelist.size()"+logdatelist.size());
            if(logdatelist.size() >= 2) {
                newdate = logdatelist.get(logdatelist.size()-1);
                System.out.println("logdatelist" + logdatelist.get(logdatelist.size()-1));//----new
                range = logdatelist.get(logdatelist.size()-1)-logdatelist.get(0);
                percentage = (double)range /(double) logdatelist.get(0) * 100;
                System.out.println("Range"+range);
                System.out.println("Percentage"+percentage);
            }
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new double[]{range, percentage,newdate};
    }
    public static double[] getPostInteraction(long startdate, long enddate, String igid,long start90date, long end90date){
        String sqlurl = "jdbc:mysql://103.254.208.224:3306/";
        String dbName = "advwhere_new";
        String driver = "com.mysql.jdbc.Driver";
        String userName = "advwhere";
        String password = "wnk6hED26mL4";
        double range = 0;
        double percentage = 0;
        double newinteraction = 0;
        int post90 = 0;
        int posts = 0;
        int comments = 0;
        int likes = 0;
        int comments90 = 0;
        int likes90 = 0;
        double interaction =0;
        double interaction90 = 0;
        List<Double> logdatelist = new ArrayList<Double>();
        List<Double> followersgrowthlist = new ArrayList<Double>();
        try {
            Class.forName(driver).newInstance();
            java.sql.Connection conn = DriverManager.getConnection(sqlurl+dbName,userName,password);
            Statement st = conn.createStatement();
            startdate = TimeUnit.MILLISECONDS.toSeconds(startdate);
            enddate = TimeUnit.MILLISECONDS.toSeconds(enddate);
            ResultSet res = st.executeQuery("SELECT * FROM ig_post where ig_user_igId = '"+igid+"'AND postDate BETWEEN  '"+startdate+"'AND'"+enddate+"'");
            while (res.next()) {
                comments += res.getInt("comments");
                likes += res.getInt("likes");
                posts += 1;
                //logdatelist.add(interaction);
            }
            conn.close();
            System.out.println("Comments"+comments);
            interaction = ((double)comments + (double)likes)/(double)posts;
            System.out.println("interaction: "+ interaction);
            java.sql.Connection conn2 = DriverManager.getConnection(sqlurl+dbName,userName,password);
            Statement st2 = conn2.createStatement();
            start90date = TimeUnit.MILLISECONDS.toSeconds(start90date);
            end90date = TimeUnit.MILLISECONDS.toSeconds(end90date);
            System.out.println("Start 90 date: "+start90date);
            System.out.println("End 90 date: "+end90date);
            ResultSet res2 = st2.executeQuery("SELECT * FROM ig_post where ig_user_igId = '"+igid+"'AND postDate BETWEEN  '"+start90date+"'AND'"+end90date+"'");
            while (res2.next()) {
                comments90 += res2.getInt("comments");
                likes90 += res2.getInt("likes");
                post90 += 1;
            }
            comments90 -= comments;
            likes90 -= likes;
            post90 -= posts;
            conn2.close();
            interaction90 = ((double)comments90 + (double)likes90)/(double)post90;
            System.out.println("90interaction: "+ interaction90);
            range = interaction - interaction90;
            if(Double.isNaN(range)){
                range = 0;
                percentage = 0;
            }else {
                percentage = (double) range / (double) interaction90 * 100;
            }
            System.out.println("Range"+range);
            System.out.println("Percentage"+percentage);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new double[]{range, percentage,interaction};
    }
    public static String[][] getMention(long startdate, long enddate, String igid){
        String sqlurl = "jdbc:mysql://localhost:3306/";
        String dbName = "advwhere";
        String driver = "com.mysql.jdbc.Driver";
        String userName = "root";
        String password = "";
        String[][] final_mentions_list = null;
        double range = 0;
        double percentage = 0;
        double newinteraction = 0;
        String mentionslist = "";
        List<Double> logdatelist = new ArrayList<Double>();
        List<Double> followersgrowthlist = new ArrayList<Double>();
        try {
            Class.forName(driver).newInstance();
            java.sql.Connection conn = DriverManager.getConnection(sqlurl+dbName,userName,password);
            Statement st = conn.createStatement();
            ResultSet res = st.executeQuery("SELECT * FROM ig_post_log where poster_igId = '"+igid+"'AND logDateTime BETWEEN  '"+startdate+"'AND'"+enddate+"'");
            while (res.next()) {
                mentionslist += res.getString("mentions");
            }
            mentionslist = mentionslist.replaceAll("\\s+$","").replaceAll("^\\s+","");
            String[] splited = mentionslist.split("\\s+");
            String[][] data = new String[splited.length][2];
            String[][] dataTest = data;
            java.util.List<Integer> geterror = new ArrayList<>();
            for(int i = 0;i <splited.length; i++){//tag name
                for(int j = 0;j<data.length;j++){//loop all str one time=
                    if(splited[i].equals(data[j][0])){
                        data[j][1] = String.valueOf(Integer.parseInt(data[j][1]) + 1);
                        geterror.add(i);
                        //data = ArrayUtils.remove(data,j);
                        break;
                    }
                    if(j == data.length - 1){
                        data[i][0] = splited[i];
                        data[i][1] = String.valueOf(1);
                    }
                }
            }
            Collections.reverse(geterror);//need to
            for(int l = 0; l < geterror.size(); l++){
                data = ArrayUtils.remove(data,geterror.get(l));
            }
            for (int r = 0; r < data.length; r++) {
                if (data[r][0] == null||data[r][1] == null) {
                    data = ArrayUtils.remove(data,r);
                    for(int j = 0; j<data.length;j++){
                    }
                }
            }
            Arrays.sort(data, new Comparator<String[]>() {
                @Override
                public int compare(final String[] entry1, final String[] entry2) {
                    final String time1 = entry1[1];
                    final String time2 = entry2[1];
                    return time2.compareTo(time1);
                }
            });
            int number = 0;
            final_mentions_list = new String[data.length][data.length];
            for (final String[] s : data) {
                int count = data.length - (data.length - number);
                number += 1;
                final_mentions_list[count][0]= s[0];
                final_mentions_list[count][1]= s[1];
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return final_mentions_list;
    }
    public static String[][] getTags(long startdate, long enddate, String igid){
        String sqlurl = "jdbc:mysql://localhost:3306/";
        String dbName = "advwhere";
        String driver = "com.mysql.jdbc.Driver";
        String userName = "root";
        String password = "";
        String[][] final_tags_list = null;
        double range = 0;
        double percentage = 0;
        double newinteraction = 0;
        String tagslist = "";
        List<Double> logdatelist = new ArrayList<Double>();
        List<Double> followersgrowthlist = new ArrayList<Double>();
        try {
            Class.forName(driver).newInstance();
            java.sql.Connection conn = DriverManager.getConnection(sqlurl+dbName,userName,password);
            Statement st = conn.createStatement();
            ResultSet res = st.executeQuery("SELECT * FROM ig_post_log where poster_igId = '"+igid+"'AND logDateTime BETWEEN  '"+startdate+"'AND'"+enddate+"'");
            while (res.next()) {
                tagslist += res.getString("tags");
            }
            tagslist = tagslist.replaceAll("\\s+$","").replaceAll("^\\s+","");;
            String[] splited = tagslist.split("\\s+");
            String[][] data = new String[splited.length][2];
            String[][] dataTest = data;
            java.util.List<Integer> geterror = new ArrayList<>();
            for(int i = 0;i <splited.length; i++){//tag name
                for(int j = 0;j<data.length;j++){//loop all str one time=
                    if(splited[i].equals(data[j][0])){
                        data[j][1] = String.valueOf(Integer.parseInt(data[j][1]) + 1);
                        geterror.add(i);
                        //data = ArrayUtils.remove(data,j);
                        break;
                    }
                    if(j == data.length - 1){
                        data[i][0] = splited[i];
                        data[i][1] = String.valueOf(1);
                    }
                }
            }
            Collections.reverse(geterror);//need to
            for(int l = 0; l < geterror.size(); l++){
                data = ArrayUtils.remove(data,geterror.get(l));
            }
            for (int r = 0; r < data.length; r++) {
                if (data[r][0] == null||data[r][1] == null) {
                    data = ArrayUtils.remove(data,r);
                }
            }
            Arrays.sort(data, new Comparator<String[]>() {
                @Override
                public int compare(final String[] entry1, final String[] entry2) {
                    final int time1 = Integer.parseInt(entry1[1]);
                    final int time2 = Integer.parseInt(entry2[1]);
                    return Integer.compare(time2,time1);
                }
            });
            int number = 0;
            final_tags_list = new String[data.length][data.length];
            for (final String[] s : data) {
                int count = data.length - (data.length - number);
                number += 1;
                final_tags_list[count][0]= s[0];
                final_tags_list[count][1]= s[1];
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return final_tags_list;
    }
    public static String[][] getEngagement(long startdate,long enddate, String igid){
        String sqlurl = "jdbc:mysql://localhost:3306/";
        String dbName = "advwhere";
        String driver = "com.mysql.jdbc.Driver";
        String userName = "root";
        String password = "";
        String[][] final_engagements_list = new String[7][3];
        double likes = 0;
        double comments = 0;
        double lifetime = 0;
        double posts = 0;
        double followers = 0;
        double engagement = 0;
        long logDateTime = 0;
        String long_mon = "0";
        String eng_mon = "0";
        String follower_mon = "0";
        String long_tue = "0";
        String eng_tue = "0";
        String follower_tue = "0";
        String long_wed = "0";
        String eng_wed = "0";
        String follower_wed = "0";
        String long_thur = "0";
        String eng_thur = "0";
        String follower_thur = "0";
        String long_fri = "0";
        String eng_fri = "0";
        String follower_fri= "0";
        String long_sat = "0";
        String eng_sat = "0";
        String follower_sat= "0";
        String long_sun = "0";
        String eng_sun = "0";
        String follower_sun = "0";
        List<Double> logdatelist = new ArrayList<Double>();
        List<Double> followersgrowthlist = new ArrayList<Double>();
        try {
            Class.forName(driver).newInstance();
            java.sql.Connection conn = DriverManager.getConnection(sqlurl+dbName,userName,password);
            Statement st = conn.createStatement();
            ResultSet res = st.executeQuery("SELECT * FROM ig_user_log where igId = '"+igid+"'AND logDateTime BETWEEN  '"+startdate+"'AND'"+enddate+"'");
            while (res.next()) {
                likes = res.getDouble("likes");
                comments = res.getDouble("comments");
                lifetime = res.getDouble("lifetime");
                followers = res.getDouble("followers");
                logDateTime = res.getLong("logDateTime");
                engagement = (comments + likes)/lifetime/followers;
                Date date = new Date(logDateTime);
                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                String pattern = "EEE";
                SimpleDateFormat format = new SimpleDateFormat(pattern, Locale.US);
                if(format.format(date).equals("Mon")&&logDateTime<=enddate){
                    long_mon = String.valueOf(logDateTime);
                    System.out.println("long_mon"+long_mon);
                    eng_mon = String.valueOf(engagement);
                    follower_mon  = String.valueOf(followers);
                }
                if(format.format(date).equals("Tue")&&logDateTime<=enddate){
                    long_tue = String.valueOf(logDateTime);
                    System.out.println("long_tue"+long_tue);
                    eng_tue = String.valueOf(engagement);
                    follower_tue  = String.valueOf(followers);
                }
                if(format.format(date).equals("Wed")&&logDateTime<=enddate){
                    long_wed = String.valueOf(logDateTime);
                    System.out.println("long_wed"+long_wed);
                    eng_wed = String.valueOf(engagement);
                    follower_wed  = String.valueOf(followers);
                }
                if(format.format(date).equals("Thu")&&logDateTime<=enddate){
                    long_thur = String.valueOf(logDateTime);
                    System.out.println("long_thur"+long_thur);
                    eng_thur = String.valueOf(engagement);
                    follower_thur  = String.valueOf(followers);
                }
                if(format.format(date).equals("Fri")&&logDateTime<=enddate){
                    long_fri = String.valueOf(logDateTime);
                    System.out.println("long_fri"+long_fri);
                    eng_fri = String.valueOf(engagement);
                    follower_fri  = String.valueOf(followers);
                }
                if(format.format(date).equals("Sat")&&logDateTime<=enddate){
                    long_sat = String.valueOf(logDateTime);
                    System.out.println("long_sat"+long_sat);
                    eng_sat = String.valueOf(engagement);
                    follower_sat  = String.valueOf(followers);
                }
                if(format.format(date).equals("Sun")&&logDateTime<=enddate){
                    long_sun = String.valueOf(logDateTime);
                    System.out.println("long_sun"+long_sun);
                    eng_sun = String.valueOf(engagement);
                    follower_sun  = String.valueOf(followers);
                }
            }
            final_engagements_list = new String[][]{{eng_mon, long_mon,follower_mon},{eng_tue, long_tue,follower_tue},{eng_wed, long_wed,follower_wed},{eng_thur, long_thur,follower_thur},{eng_fri, long_fri,follower_fri},{eng_sat, long_sat,follower_sat},{eng_sun, long_sun,follower_sun}};
        } catch (Exception e) {
            e.printStackTrace();
        }
        return final_engagements_list;
    }
    public static long FirstDay(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.HOUR, -12);

        return calendar.getTimeInMillis();
    }
    public static long LastDay(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.MILLISECOND, 59);
        calendar.set(Calendar.SECOND, 59);
        calendar.set(Calendar.MINUTE, 59);
        calendar.set(Calendar.HOUR, 11);

        return calendar.getTimeInMillis();
    }
    public static String exportJSON(double[] followersgrowth, double[] postinteraction, long startdate, long enddate){
        org.json.simple.JSONObject main = new org.json.simple.JSONObject();
        org.json.simple.JSONObject top5 = new org.json.simple.JSONObject();
        org.json.simple.JSONObject rating = new org.json.simple.JSONObject();
        org.json.simple.JSONObject followers = new org.json.simple.JSONObject();
        org.json.simple.JSONObject post = new org.json.simple.JSONObject();
        org.json.simple.JSONObject profile = new org.json.simple.JSONObject();
        org.json.simple.JSONObject tags = new org.json.simple.JSONObject();
        org.json.simple.JSONObject follower = new org.json.simple.JSONObject();
        org.json.simple.JSONObject date = new org.json.simple.JSONObject();
        org.json.simple.JSONObject mentions = new org.json.simple.JSONObject();
        JSONArray perfomance = new JSONArray();
        org.json.simple.JSONObject performance1 = new org.json.simple.JSONObject();
        main.put("rating",rating);
        double total_tags = 0;
        rating.put("start_time", startdate);
        rating.put("end_time", enddate);
        rating.put("followers", followers);
        for(int i = 0; i < followersgrowth.length; i++){
            followers.put("rational number", Integer.parseInt(String.valueOf(followersgrowth[0]).replace(".0","")));
            followers.put("percentage", Double.parseDouble(String.valueOf(followersgrowth[1])));
            followers.put("totalfollowers", Integer.parseInt(String.valueOf(followersgrowth[2]).replace(".0","")));
        }
        rating.put("post", post);
        for(int i = 0; i < postinteraction.length; i++){
            post.put("post interaction", (int)Double.parseDouble(String.valueOf(postinteraction[0])));
            System.out.println("Double Value:"+Double.parseDouble(String.valueOf(postinteraction[0])));
            post.put("percentage", Double.parseDouble(String.valueOf(postinteraction[1])));
            post.put("totalinteraction", (int)Double.parseDouble(String.valueOf(postinteraction[2])));
            System.out.println("totalinteraction"+Double.parseDouble(String.valueOf(postinteraction[2])));
        }
       /* Map<String,JSONObject> n = new HashMap<String,JSONObject>();
        for(int i = 0; i < engagment.length; i++){
            n.put("engagement"+i,new JSONObject());
        }
        for(int i = 0; i< n.size();i++){
            n.get("engagement"+i).put("engagement", Double.parseDouble(engagment[i][0]));
            try {
                n.get("engagement" + i).put("followers", Long.parseLong(engagment[i][2]));
            }catch (NumberFormatException e){
                n.get("engagement" + i).put("followers", Long.parseLong(engagment[i][2].replace(".0","")));
            }
            n.get("engagement"+i).put("date", Long.parseLong(engagment[i][1]));
            perfomance.add(n.get("engagement"+i));
        }*/
        return main.toString();
    }


}
