package za.botneil.toiletwars;

import org.bukkit.entity.Player;
import org.bukkit.scheduler.BukkitRunnable;

import java.sql.*;
import java.util.HashMap;
import java.util.UUID;

public class mariadb {
    private Connection connection;
    private String host, database, username, password;
    private int port;
    private Toiletwars toiletwars;
    private  static HashMap<String, LeaderBoardComparable> topkillers = new HashMap();
    private  static HashMap<String, LeaderBoardComparable> topdeaths = new HashMap();
    public static HashMap<String, LeaderBoardComparable> getTopkillers(){return topkillers;}
    public static HashMap<String, LeaderBoardComparable> getTopDeaths(){return topdeaths;}
    public void initmysql(Toiletwars toiletwars) throws SQLException, ClassNotFoundException {
        this.toiletwars = toiletwars;
        host = toiletwars.getConfig().getString("MySQL.host");
        port = toiletwars.getConfig().getInt("MySQL.port");
        database = toiletwars.getConfig().getString("MySQL.database");
        username = toiletwars.getConfig().getString("MySQL.username");
        password = toiletwars.getConfig().getString("MySQL.password");
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
            //System.out.println("jdbc:mysql://" + this.host+ ":" + this.port + "/" + this.database, this.username, this.password);
            //?autoReconnect=true
            createTable();
        }
    }
    public void createTable() throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS toiletwars (Id INT AUTO_INCREMENT PRIMARY KEY,UUID CHAR(36) NOT NULL UNIQUE, NAME CHAR(36) NOT NULL,coins INT,kills INT,deaths INT,finalkills INT,wins INT,modifier INT,projectiles_launched INT,projectiles_hit INT,player_exp INT, player_rank INT, blocks_placed INT, blocks_broken INT,cores_broken INT,playtime varchar(90), inventory varchar(1300), selecteditems varchar(50));";
        try {
            connection.prepareStatement(sql).executeUpdate();
        }catch (SQLException e){
            e.printStackTrace();
        }
    }
    public void loadPlayerdata(PlayerData pd,Player player) throws SQLException {
        BukkitRunnable r = new BukkitRunnable() {
            @Override
            public void run() {
                try {
                    String inventory = "";
                    String selecteditems = "";
                    Statement statement = connection.createStatement();
                    ResultSet result = statement.executeQuery("SELECT * FROM icewars WHERE UUID = '"+player.getUniqueId().toString()+"';");
                    if(result.next()){
                        pd.databaseID = result.getInt("Id");
                        pd.coins=  result.getInt("coins");;
                        pd.kills = result.getInt("kills");
                        pd.deaths= result.getInt("deaths");
                        pd.finalKills =result.getInt("finalkills");
                        pd.wins =result.getInt("wins");
                        pd.modifier =result.getInt("modifier");
                        pd.projectiles_launched = result.getInt("projectiles_launched");
                        pd.projectiles_hit= result.getInt("projectiles_hit");
                        pd.player_exp= result.getInt("player_exp");
                        pd.blocks_placed=result.getInt("blocks_placed");
                        pd.blocks_broken=result.getInt("blocks_broken");
                        pd.eggs_broken=result.getInt("cores_broken");
                        pd.loadPlayTime(result.getString("playtime"));
                        inventory = result.getString("inventory");
                        selecteditems = result.getString("selecteditems");
                        pd.loadInventory(inventory);
                        pd.loadSelectedItems(selecteditems);
                    }else{
                        createPlayer(pd,player);
                    }
                }catch (SQLException e){
                    e.printStackTrace();
                }
            }
        };
        r.runTaskAsynchronously(this.toiletwars);

    }
    public void savePlayerDataSync(PlayerData pd){
        try {
            String sql = "UPDATE `icewars`" +
                    " SET `NAME` = '"+ pd.name
                    +"', `coins` = '" +pd.coins
                    +"', `kills` = '" +pd.kills
                    +"', `deaths` = '" +pd.deaths
                    +"', `finalkills` = '" +pd.finalKills
                    +"', `wins` = '" +pd.wins
                    +"', `modifier` = '" +pd.modifier
                    +"', `projectiles_launched` = '" +pd.projectiles_launched
                    +"', `projectiles_hit` = '" +pd.projectiles_hit
                    +"', `player_exp` = '" +pd.player_exp
                    +"', `player_rank` = '" +pd.player_rank
                    +"', `blocks_placed` = '" +pd.blocks_placed
                    +"', `blocks_broken` = '" +pd.blocks_broken
                    +"', `cores_broken` = '" +pd.eggs_broken
                    +"', `playtime` = '" +pd.getPlayTime()
                    +"', `inventory` = '" +pd.getInventory()
                    +"', `selecteditems` = '" +pd.getSelectedItems()
                    +"'  WHERE `icewars`.`Id`  = "+pd.databaseID+";";
            //System.out.println(sql);
            connection.prepareStatement(sql).executeUpdate();
        }catch (SQLException e){
            e.printStackTrace();
        }
    }
    public void savePlayerData(PlayerData pd){
        BukkitRunnable r = new BukkitRunnable() {
            @Override
            public void run() {
                try {
                    String sql = "UPDATE `icewars`" +
                            " SET `NAME` = '"+ pd.name
                            +"', `coins` = '" +pd.coins
                            +"', `kills` = '" +pd.kills
                            +"', `deaths` = '" +pd.deaths
                            +"', `finalkills` = '" +pd.finalKills
                            +"', `wins` = '" +pd.wins
                            +"', `modifier` = '" +pd.modifier
                            +"', `projectiles_launched` = '" +pd.projectiles_launched
                            +"', `projectiles_hit` = '" +pd.projectiles_hit
                            +"', `player_exp` = '" +pd.player_exp
                            +"', `player_rank` = '" +pd.player_rank
                            +"', `blocks_placed` = '" +pd.blocks_placed
                            +"', `blocks_broken` = '" +pd.blocks_broken
                            +"', `cores_broken` = '" +pd.eggs_broken
                            +"', `playtime` = '" +pd.getPlayTime()
                            +"', `inventory` = '" +pd.getInventory()
                            +"', `selecteditems` = '" +pd.getSelectedItems()
                            +"'  WHERE `icewars`.`Id`  = "+pd.databaseID+";";
                    //System.out.println(sql);
                    connection.prepareStatement(sql).executeUpdate();
                }catch (SQLException e){
                    e.printStackTrace();
                }
            }
        };
        r.runTaskAsynchronously(this.toiletwars);
    }
    public void updatePlayerstat(String playername, String stat,int amount){
        BukkitRunnable r = new BukkitRunnable() {
            @Override
            public void run() {
                try {
                    String sql = "UPDATE `icewars` SET `"+stat+"` = '"+amount +"'  WHERE `icewars`.`NAME`  = "+playername+";";
                    //System.out.println(sql);
                    connection.prepareStatement(sql).executeUpdate();
                }catch (SQLException e){
                    e.printStackTrace();
                }
            }
        };
        r.runTaskAsynchronously(this.toiletwars);
    }
    public void resetPlayer(String playername){
        BukkitRunnable r = new BukkitRunnable() {
            @Override
            public void run() {
                try {
                    String sql = "DELETE from `icewars`  WHERE `icewars`.`NAME`  = "+playername+";";
                    //System.out.println(sql);
                    connection.prepareStatement(sql).executeUpdate();
                }catch (SQLException e){
                    e.printStackTrace();
                }
            }
        };
        r.runTaskAsynchronously(this.toiletwars);
    }
    public void createPlayer(PlayerData playerData,Player player) throws SQLException {
        BukkitRunnable r = new BukkitRunnable() {
            @Override
            public void run() {
                try {
                    String sql ="INSERT INTO icewars (UUID, NAME) VALUES ('"+player.getUniqueId().toString()+"', '"+ player.getName() +"');";
                    connection.prepareStatement(sql).executeUpdate();

                }catch (SQLException e){
                    e.printStackTrace();
                }
            }
        };
        r.runTaskLaterAsynchronously(this.toiletwars, 2);
        BukkitRunnable r2 = new BukkitRunnable() {
            @Override
            public void run() {
                try {
                    loadPlayerdata(playerData,player);
                } catch(SQLException e) {
                    e.printStackTrace();
                }
            }
        };
        r2.runTaskLaterAsynchronously(this.toiletwars, 20);
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
                            "FROM `icewars`" +
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
        r.runTaskAsynchronously(this.toiletwars);
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
                            "FROM `icewars`" +
                            "ORDER BY `DEATHS` DESC " +
                            "LIMIT 10;");
                    while(result.next()){
                        amount = result.getInt("DEATHS");
                        name = result.getString("NAME");
                        topkillers.put(name,new LeaderBoardComparable(name, amount));
                    }
                }catch (SQLException e){
                    e.printStackTrace();
                }
            }
        };
        r.runTaskAsynchronously(this.toiletwars);
    }
}
