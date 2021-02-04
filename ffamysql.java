package me.pafias.ffa.ffa;

import me.pafias.ffa.ffa.leaderboards.LeaderBoardComparable;
import org.bukkit.entity.Player;
import org.bukkit.scheduler.BukkitRunnable;

import java.sql.*;
import java.util.HashMap;
import java.util.UUID;

public class mysql {
    private Connection connection;
    private String host, database, username, password;
    private int port;
    private  static HashMap<String, LeaderBoardComparable> topkillers = new HashMap();
    private  static HashMap<String, LeaderBoardComparable> topdeaths = new HashMap();
    private static HashMap<String,HashMap<Integer,LeaderBoardComparable>>topStatsbyNameandAmount = new HashMap<>();
    public static HashMap<String, HashMap<Integer, LeaderBoardComparable>>getTopStatsByNameandAmount(){return topStatsbyNameandAmount;}
    public static HashMap<String, LeaderBoardComparable> getTopkillers(){return topkillers;}
    public static HashMap<String, LeaderBoardComparable> getTopDeaths(){return topdeaths;}
    public void initmysql() throws SQLException, ClassNotFoundException {
        host = "127.0.0.1";//shouldn't be hardcoded but only I use it.
        port = 3306;
        database = "ffa";
        username = "user";
        password = "user";
        openConnection();
    }
    public void openConnection() throws SQLException, ClassNotFoundException {
        if (connection != null && !connection.isClosed()) {
            return;
        }

        synchronized (this) {
            if (connection != null && !connection.isClosed()) {
                return;
            }
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://" + this.host+ ":" + this.port + "/" + this.database+"?autoReconnect=true", this.username, this.password);
            createTable();
        }
    }
    public void createTable() throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS ffa (Id INT AUTO_INCREMENT PRIMARY KEY,UUID CHAR(36) NOT NULL UNIQUE, NAME CHAR(36) NOT NULL, KILLS INT, DEATHS INT, KILLSTREAK INT, DAILYKILLS INT,DAILYDEATHS INT, MONTHLYKILLS INT,MONTHLY DEATHS);";
        try {
            connection.prepareStatement(sql).executeUpdate();
        }catch (SQLException e){
            e.printStackTrace();
        }
    }
    public void fetchUser(Player player) throws SQLException {
        BukkitRunnable r = new BukkitRunnable() {
            @Override
            public void run() {
                try {
                    UUID uuid;
                    String name;
                    int id = 0;
                    int totkills = 0;
                    int totdeaths = 0;
                    int maxkillstreak = 0;
                    int dailyKills = 0;
                    int dailyDeaths = 0;
                    int monthlyKills = 0;
                    int monthlyDeaths = 0;
                    Statement statement = connection.createStatement();
                    ResultSet result = statement.executeQuery("SELECT * FROM ffa WHERE UUID = '"+player.getUniqueId()+"';");
                    if(result.next()){
                        id = result.getInt("Id");
                        name = result.getString("NAME");
                        totkills = result.getInt("KILLS");
                        totdeaths = result.getInt("DEATHS");
                        maxkillstreak = result.getInt("KILLSTREAK");
                        dailyKills = result.getInt("DAILYKILLS");
                        dailyDeaths = result.getInt("DAILYDEATHS");
                        monthlyKills = result.getInt("MONTHLYKILLS");
                        monthlyDeaths = result.getInt("MONTHLYDEATHS");
                        //System.out.println("debug: "+id+name+totkills+totdeaths+maxkillstreak);
                        Users.addUser(player.getUniqueId(), new User(id, player, totkills, totdeaths, 0,maxkillstreak, dailyKills, dailyDeaths,monthlyKills,monthlyDeaths,true));
                    }else{
                        createPlayer(player);
                    }
                }catch (SQLException e){
                    e.printStackTrace();
                }
            }
        };
        r.runTaskAsynchronously(FFA.getInstance());

    }
    public void saveUser(User u){
        BukkitRunnable r = new BukkitRunnable() {
            @Override
            public void run() {
                try {
                    Player player = u.getPlayer();
                    String sql = "UPDATE `ffa`" +
                            " SET `NAME` = '"+ player.getName()
                            + "', `KILLS` = '" +u.getTotalKills()
                            +"', `DEATHS` = '" +u.getTotalDeaths()
                            +"', `KILLSTREAK` = '" +u.getMaxKillstreak()
                            +"', `DAILYKILLS` = '" +u.getDailyKills()
                            +"', `DAILYDEATHS` = '" +u.getDailyDeaths()
                            +"', `MONTHLYKILLS` = '" +u.getMonthlyKills()
                            +"', `MONTHLYDEATHS` = '" +u.getMonthlyDeaths()
                            +"' " + " WHERE `ffa`.`Id`  = "+u.getId()+";";
                    //System.out.println(sql);
                    connection.prepareStatement(sql).executeUpdate();
                }catch (SQLException e){
                    e.printStackTrace();
                }
            }
        };
        r.runTaskAsynchronously(FFA.getInstance());
    }
    public void createPlayer(Player player) throws SQLException {
        BukkitRunnable r = new BukkitRunnable() {
            @Override
            public void run() {
                try {
                    String sql ="INSERT INTO ffa (UUID, NAME) VALUES ('"+player.getUniqueId().toString()+"', '"+ player.getName() +"');";
                    connection.prepareStatement(sql).executeUpdate();

                }catch (SQLException e){
                    e.printStackTrace();
                }
            }
        };
        r.runTaskLaterAsynchronously(FFA.getInstance(), 2);
        BukkitRunnable r2 = new BukkitRunnable() {
            @Override
            public void run() {
                try {
                    fetchUser(player);
                } catch(SQLException e) {
                    e.printStackTrace();
                }
            }
        };
        r2.runTaskLaterAsynchronously(FFA.getInstance(), 20);
    }
    public void resetDailystats(){
        BukkitRunnable r = new BukkitRunnable() {
            @Override
            public void run() {
                Users.getUsers().values().forEach(x->{
                    x.setDailyKills(0);
                    x.setDailyDeaths(0);
                });
                try {
                    String sql ="UPDATE ffa set `DAILYKILLS` = DEFAULT,`DAILYDEATHS` = DEFAULT";
                    connection.prepareStatement(sql).executeUpdate();

                }catch (SQLException e){
                    e.printStackTrace();
                }
            }
        };
        r.runTaskAsynchronously(FFA.getInstance());
    }
    public void resetMonthlystats(){
        BukkitRunnable r = new BukkitRunnable() {
            @Override
            public void run() {
                Users.getUsers().values().forEach(x->{
                    x.setMonthlyKills(0);
                    x.setMonthlyDeaths(0);
                });
                try {
                    String sql ="UPDATE ffa set `MONTHLYKILLS` = DEFAULT,`MONTHLYDEATHS` = DEFAULT";
                    connection.prepareStatement(sql).executeUpdate();

                }catch (SQLException e){
                    e.printStackTrace();
                }
            }
        };
        r.runTaskAsynchronously(FFA.getInstance());
    }
    public void fetchTopKillers() throws SQLException {
        BukkitRunnable r = new BukkitRunnable() {
            @Override
            public void run() {
                try {
                    String name;
                    int kills = 0;
                    Statement statement = connection.createStatement();
                    ResultSet result = statement.executeQuery("SELECT `NAME`, `KILLS`" +
                            "FROM `ffa`" +
                            "ORDER BY `KILLS` DESC " +
                            "LIMIT 10;");
                    while(result.next()){
                        kills = result.getInt("KILLS");
                        name = result.getString("NAME");
                        topkillers.put(name,new LeaderBoardComparable(name, kills));
                    }
                }catch (SQLException e){
                    e.printStackTrace();
                }
            }
        };
        r.runTaskAsynchronously(FFA.getInstance());
    }
    public void fetchTopDeaths() throws SQLException {
        BukkitRunnable r = new BukkitRunnable() {
            @Override
            public void run() {
                try {
                    String name;
                    int amount = 0;
                    Statement statement = connection.createStatement();
                    ResultSet result = statement.executeQuery("SELECT `NAME`, `DEATHS`" +
                            "FROM `ffa`" +
                            "ORDER BY `DEATHS` DESC " +
                            "LIMIT 10;");
                    while(result.next()){
                        amount = result.getInt("DEATHS");
                        name = result.getString("NAME");
                        topdeaths.put(name,new LeaderBoardComparable(name, amount));
                    }
                }catch (SQLException e){
                    e.printStackTrace();
                }
            }
        };
        r.runTaskAsynchronously(FFA.getInstance());
    }
}
