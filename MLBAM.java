import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.TreeSet;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.io.FileWriter;
import java.util.Iterator;
import java.net.URL;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.net.URLDecoder;
import java.util.Scanner;
import java.sql.ResultSet;

public class MLBAM{
	private static final String host = "localhost";
	private static final String port = "1433";
	private static final String user = "user";
	private static final String password = "password";
	private static final String database = "MLBAM";
	private static final String MLBAMdelimiter = ";";
	private static final String MLBAMrowTerminator = "\\n";
	
	private String xmlFolder;
	private String txtFolder;
	private String sqlFolder;
	
	private String strDay = "";
	private String strMonth = "";
    private String strYear = "";
    private String strDate = "";
    
    private HashMap<String,String> gameIDsURLs = new HashMap<String,String>();
	
	public static void main(String args[]){
		MLBAM mlbam = new MLBAM();
		mlbam.run();	
	}
	
	public MLBAM(){
		
	}
	
	public void run(){
		System.out.println("MLBAM is running...");
		
		//if overwrite = false, then before a file is downloaded, MLBAM checks to see if file already exists, and if so does not download the file again.
		//if overwrite = true, then a file is downloaded regardless of whether it already exists
		boolean overwrite = false;
		//when this is false, MLBAM checks gamesInserted table to see if previous days games have already been insered into SQL. If the games have according to
		//that table, then the data is not inserted again, otherwise the data is inserted. If set to true, then the data is inserted regardless
		boolean dontDuplicate = true;
		
		//not used currently, but idea was to keep track of errors occured during parsing of XML files and sending data to SQL
		StringBuilder errorLog = new StringBuilder();

		//set the previous days date and the folder path to the where the XML and txt files will be stored based on MLBAM class directory
		setDateAndFolderPaths();
		//creates folders if they don't already exist
		createFolders();
		//creates tables if they don't already exist
		createTables();
		//downloads XML file that contains links to all the previous days games
		downloadMasterScorecard(overwrite);
		//determine ID's and links for previous days games
		getGamesPlayed();
		//download 5 XML files per game
		downloadGameFiles(overwrite);
		//parse all XML files for previous days games and create 4 csv's to be loaded into SQL
		createMLBAMcsv(errorLog);
		//import previously created csv's into SQL
		sendMLBAMDataToSQL(dontDuplicate,errorLog);
	}
	
	//returns true if a table exists called tableName, false otherwise
	public boolean tableExists(String tableName){
		ResultSet rs;
		String query;
		StringBuilder errorLog = new StringBuilder();
		SendDataToSQL sdts = new SendDataToSQL();
		String result = "";
		
		query = "IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '" + tableName + "') select 'true' as result else select 'false' as result";
		rs = sdts.getQuery(host, port, user, password, database, query, errorLog);
		try{
			while(rs.next()){
				result = rs.getString("result");
			}
			rs.close();
		}catch(Exception e){
			System.out.println(e);
		}
		
		if(result.equals("true")){
			return true;
		}else{
			return false;
		}
	}
	
	//checks if tables already exist, and if not creates that table
	public void createTables(){
		StringBuilder errorLog = new StringBuilder();
		SendDataToSQL sdts = new SendDataToSQL();
		boolean exists;
		String tableName;
		String query;
		String filePath;
		
		//mlb_gameday_actions
		tableName = "mlb_gameday_actions";
		exists = tableExists(tableName);
		if(!exists){
			filePath = sqlFolder + "Create mlb_gameday_actions.sql";
			try{
				query = new Scanner(new InputStreamReader(new FileInputStream(filePath), "UTF8")).useDelimiter("\\Z").next();
				sdts.executeQuery(host, port, user, password, database, query, errorLog);
			}catch(Exception e){
				System.out.println(e);
			}
		}
		
		//mlb_gameday_at_bats
		tableName = "mlb_gameday_at_bats";
		exists = tableExists(tableName);
		if(!exists){
			filePath = sqlFolder + "Create mlb_gameday_at_bats.sql";
			try{
				query = new Scanner(new InputStreamReader(new FileInputStream(filePath), "UTF8")).useDelimiter("\\Z").next();
				sdts.executeQuery(host, port, user, password, database, query, errorLog);
			}catch(Exception e){
				System.out.println(e);
			}
		}
		
		//mlb_gameday_games
		tableName = "mlb_gameday_games";
		exists = tableExists(tableName);
		if(!exists){
			filePath = sqlFolder + "Create mlb_gameday_games.sql";
			try{
				query = new Scanner(new InputStreamReader(new FileInputStream(filePath), "UTF8")).useDelimiter("\\Z").next();
				sdts.executeQuery(host, port, user, password, database, query, errorLog);
			}catch(Exception e){
				System.out.println(e);
			}
		}
		
		//mlb_gameday_players
		tableName = "mlb_gameday_players";
		exists = tableExists(tableName);
		if(!exists){
			filePath = sqlFolder + "Create mlb_gameday_players.sql";
			try{
				query = new Scanner(new InputStreamReader(new FileInputStream(filePath), "UTF8")).useDelimiter("\\Z").next();
				sdts.executeQuery(host, port, user, password, database, query, errorLog);
			}catch(Exception e){
				System.out.println(e);
			}
		}
		
		//gamesInserted
		tableName = "gamesInserted";
		exists = tableExists(tableName);
		if(!exists){
			filePath = sqlFolder + "Create gamesInserted.sql";
			try{
				query = new Scanner(new InputStreamReader(new FileInputStream(filePath), "UTF8")).useDelimiter("\\Z").next();
				sdts.executeQuery(host, port, user, password, database, query, errorLog);
			}catch(Exception e){
				System.out.println(e);
			}
		}
	}
	
	//downloads 5 XML files for each game
	public void downloadGameFiles(boolean overwrite){
		Iterator itr;
		String gameID;
		String gameBaseURL;	
		String filePath;
		String url;
		
		itr = gameIDsURLs.keySet().iterator();
		while(itr.hasNext()){
			gameID = (String) itr.next();
			gameBaseURL = gameIDsURLs.get(gameID);
			
			url = gameBaseURL + "/game.xml";
			filePath = xmlFolder + gameID + "_game.xml";
			downloadXMLfile(url,filePath,overwrite);
			
			url = gameBaseURL + "/game_events.xml";
			filePath = xmlFolder + gameID + "_game_events.xml";
			downloadXMLfile(url,filePath,overwrite);
			
			url = gameBaseURL + "/linescore.xml";
			filePath = xmlFolder + gameID + "_linescore.xml";
			downloadXMLfile(url,filePath,overwrite);
			
			url = gameBaseURL + "/players.xml";
			filePath = xmlFolder + gameID + "_players.xml";
			downloadXMLfile(url,filePath,overwrite);
			
			url = gameBaseURL + "/plays.xml";
			filePath = xmlFolder + gameID + "_plays.xml";
			downloadXMLfile(url,filePath,overwrite);
		}
	}
	
	public void setDateAndFolderPaths(){
		String basePath = getBasePath();
		int day;
		int month;
		int year;
        Calendar cal = Calendar.getInstance();
        
		cal.add(Calendar.DATE, -1);    
        day = cal.get(Calendar.DAY_OF_MONTH);
        if(day < 10){
	        strDay = "0" + day;
        }else{
	        strDay = String.valueOf(day);
        }
        month = cal.get(Calendar.MONTH) + 1;
        if(month < 10){
	        strMonth = "0" + month;
        }else{
	        strMonth = String.valueOf(month);
        }
        year = cal.get(Calendar.YEAR);
        strYear = String.valueOf(year);
        strDate = year + strMonth + strDay;
		
        xmlFolder = basePath + "XML/" + strDate + "/";
        txtFolder = basePath + "TXT/" + strDate + "/";
        sqlFolder = basePath + "SQL/";
	}
	
	//creates folder for yesterdays games in TXT and XML folders
	public void createFolders(){
		File file;
        Process p;
        String command;
        
        file = new File(xmlFolder);
        if(file.exists() && file.isDirectory()){
	        //don't do anything, folder already exists
        }else{
	        try{
		        command = "cmd /c mkdir \"" + file.getAbsolutePath() + "\"";
				System.out.println(command);
				p = Runtime.getRuntime().exec(command);
	        }catch(Exception e){
		        System.out.println(e);
	        }
        }
        
        file = new File(txtFolder);
        if(file.exists() && file.isDirectory()){
	        //don't do anything, folder already exists
        }else{
	        try{
		        command = "cmd /c mkdir \"" + file.getAbsolutePath() + "\"";
				System.out.println(command);
				p = Runtime.getRuntime().exec(command);
	        }catch(Exception e){
		        System.out.println(e);
	        }
        }
	}
	
	//get the path to the class file or jar file that is being run
	public String getBasePath(){
		int pos;
		String basePath = "";
		String path;
		String decodedPath = "";
		File file;
		
		try{
			path = MLBAM.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
			decodedPath = URLDecoder.decode(path, "UTF-8");
		}catch(Exception e){
			System.out.println(e);
		}
	
		basePath = decodedPath;
		if(basePath.contains("/C:/")){
		    pos = basePath.indexOf("/C:/");
		    basePath = basePath.substring(pos+1);
	    }
		
		file = new File(basePath);
		//if program is being run from a jar, then file will be location of jar file. Otherwise, it should be the correct basePath
		if(file.isFile()){
			basePath = 	file.getParentFile().getPath();
			basePath = basePath.replace("\\","/");
			basePath = basePath + "/";
		}
		
		return basePath;
	}
	
	//downloads XML file which contains IDs and links for all of yesterday's games
	public void downloadMasterScorecard(boolean overwrite){
		String url = "http://gd2.mlb.com/components/game/mlb/year_" + strYear + "/month_" + strMonth + "/day_" + strDay + "/master_scoreboard.xml";
		String filePath = xmlFolder + strDate + "_master_scoreboard.xml";
		downloadXMLfile(url,filePath,overwrite);
	}
	
	//parses XML to get each game ID and its corresponding link
	public void getGamesPlayed(){
		//building xml files in memory
		File fXmlFile;
		DocumentBuilderFactory dbFactory;
		DocumentBuilder dBuilder;
		Document doc;
		NodeList nList;
		Node nNode;
		Element eElement;
		
		String gameID;
		String gameBaseURL;
		
		Iterator itr;
        
		String filePath = xmlFolder + strDate + "_master_scoreboard.xml";
					
		try{
			fXmlFile = new File(filePath);
			dbFactory = DocumentBuilderFactory.newInstance();
			dBuilder = dbFactory.newDocumentBuilder();
			doc = dBuilder.parse(fXmlFile);
			
			//optional, but recommended
			//read this - http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
			doc.getDocumentElement().normalize();
	
			nList = doc.getElementsByTagName("game");
			for (int i = 0; i < nList.getLength(); i++){
				nNode = nList.item(i);
				eElement = (Element) nNode;
				gameID = eElement.getAttribute("gameday");
				gameBaseURL = "http://gd2.mlb.com" + eElement.getAttribute("game_data_directory");
				gameIDsURLs.put(gameID,gameBaseURL);
			}
		}catch(Exception e){
			System.out.println(e);	
		}
	}
			
	//returns true if data has already been sent for yesterday according to gamesInserted table
	public boolean dataAlreadySent(){
		ResultSet rs;
		String query;
		StringBuilder errorLog = new StringBuilder();
		SendDataToSQL sdts = new SendDataToSQL();
		String result = "";
		String curDate;
		
		curDate = strYear + "-" + strMonth + "-" + strDay;
		query = "select * from gamesInserted where date = '" + curDate + "'";
		rs = sdts.getQuery(host, port, user, password, database, query, errorLog);
		try{
			while(rs.next()){
				result = rs.getString("inserted");
			}
			rs.close();
		}catch(Exception e){
			System.out.println(e);
		}
		
		if(result.equals("yes")){
			return true;
		}else{
			return false;
		}
	}
	
	//inserts game data to SQL and also inserts into gamesInserted to keep track of fact that data has been inserted for yesterday
	public int sendMLBAMDataToSQL(boolean dontDuplicate, StringBuilder errorLog){
		SendDataToSQL sdts = new SendDataToSQL();
		int returnValue = -1;
		
		//these files will be inserted into db
		String actionsCSVFile = "";
		String atBatsCSVFile = "";
		String gamesCSVFile = "";
		String playersCSVFile = "";
		
		String tableName;
		boolean sendData;
		String curDate;
		String query;
		
		if(dontDuplicate){
			if(dataAlreadySent()){
				sendData = false;	
			}else{
				sendData = true;
			}
		}else{
			sendData = true;
		}
		
		if(sendData){
			atBatsCSVFile = txtFolder + strDate + "_mlb_gameday_at_bats.txt";
			actionsCSVFile = txtFolder + strDate + "_mlb_gameday_actions.txt";
			gamesCSVFile = txtFolder + strDate + "_mlb_gameday_games.txt";
			playersCSVFile = txtFolder + strDate + "_mlb_gameday_players.txt";
			
			tableName = "mlb_gameday_at_bats";
			sdts.bulkInsert(atBatsCSVFile,host,port,user,password,database,tableName,MLBAMdelimiter,MLBAMrowTerminator,errorLog);
			tableName = "mlb_gameday_actions";
			sdts.bulkInsert(actionsCSVFile,host,port,user,password,database,tableName,MLBAMdelimiter,MLBAMrowTerminator,errorLog);
			tableName = "mlb_gameday_games";
			sdts.bulkInsert(gamesCSVFile,host,port,user,password,database,tableName,MLBAMdelimiter,MLBAMrowTerminator,errorLog);
			tableName = "mlb_gameday_players";
			sdts.bulkInsert(playersCSVFile,host,port,user,password,database,tableName,MLBAMdelimiter,MLBAMrowTerminator,errorLog);
			
			//keep track of fact that data has been inserted for this day
			curDate = strYear + "-" + strMonth + "-" + strDay;
			query = "insert into gamesInserted values ('" + curDate + "','yes')";
			sdts.executeQuery(host, port, user, password, database, query, errorLog);
		}
		
		if(errorLog.toString().equals("")){
			errorLog.append("sendMLBAMDataToSQL() completed successfully!");
			returnValue = 0;
		}
		
		return returnValue;
	}
	
	
	//will loop through every Game ID, and try to grab the 5 necessary XML files from xmlFolder and then save 4 csv's (one for each table) in the txtFolder
	public int createMLBAMcsv(StringBuilder errorLog){
		//return 0 if success, another number if error
		int returnValue = -1;
		
		//to iterate over game id's
		Iterator itr;
		String curGID;
		
		//file paths for inputs (xml files) and outputs (csv's)
		String gameXML;
		String gameEventsXML;
		String lineScoreXML;
		String playersXML;
		String playsXML;
		String actionsCSVFile = txtFolder + strDate + "_mlb_gameday_actions.txt";
		String atBatsCSVFile = txtFolder + strDate + "_mlb_gameday_at_bats.txt";
		String gamesCSVFile = txtFolder + strDate + "_mlb_gameday_games.txt";
		String playersCSVFile = txtFolder + strDate + "_mlb_gameday_players.txt";
		
		//used to check if XML file exists for given game id
		File gameXMLFile;
		File gameEventsXMLFile;
		File lineScoreXMLFile;
		File playersXMLFile;	
		File playsXMLFile;
		
		//for writing csv's
		BufferedWriter bwActions;
		BufferedWriter bwAtBats;
		BufferedWriter bwGames;
		BufferedWriter bwPlayers;
		String curWord;	//temp storage for each word being written to csv so that it can be checked for special characters (mostly delimiter)
		
		//building xml files in memory
		File fXmlFile;
		DocumentBuilderFactory dbFactory;
		DocumentBuilder dBuilder;
		Document docGame;
		Document docGameEvents;
		Document docLinescore;
		Document docPlayers;
		Document docPlays;
		NodeList nList;
		NodeList nList2;
		NodeList nList3;
		Node nNode;
		Element eElement;
		
		//columns
		//mlb_gameday_games
		String gid = "";
		String wind = "";
		String temp = "";
		String dome = "";
		String local_game_time = "";
		String game_time_et = "";
		String home_id = "";
		String away_id = "";
		String game_type = "";
		String stadium_id = "";
		String venue_w_chan_loc = "";
		String umpire_hp_id  = "";
		String umpire_1b_id = "";
		String umpire_2b_id = "";
		String umpire_3b_id = "";
		String home_dh_id = "";
		String away_dh_id = "";
		String league_id = "";
		String sport_code = "";
		String league = "";
		//mlb_gameday_players
		String gameday_id;
		String first;
		String last;
		String number;
		String boxname;
		String position;
		String current_position;
		String strThrows;
		String status;
		String team_id;
		String game_id;
		String bats;
		//mlb_gameday_actions		
		//String game_id; //already defined for mlb_gameday_players
		String balls;
		String strikes;
		String outs;
		String des;
		String strEvent;
		String player_id;
		String pitch_number;
		String tfs_zulu;
		String event_num;
		String at_bat_num;
		//mlb_gameday_at_bats
		//String game_id;	//already defined for mlb_gameday_players
		String inning;
		String num;
		String ball;
		String strike;
		String outsAtBat;
		String batter_id;
		String pitcher_id;
		String desString;
		String eventAtBat;
		String on_first;
		String on_second;
		String on_third;
		String pitcher_role;
		String catcher_id;
		String is_top = "";
		String first_id;
		String second_id;
		String third_id;
		String ss_id;
		String lf_id;
		String rf_id;
		String cf_id;
		
		
		//helper storage (so we can write to file once per game)
		//mlb_gameday_games
		ArrayList<String> curGame_mlb_gameday_games;
		final int numCols_mlb_gameday_games = 24;	//this is a constant based on table and should not change
		//mlb_gameday_players
		ArrayList<List<String>> curGame_mlb_gameday_players;
		List<String> curPlayer_mlb_gameday_players;
		int numPlayers;
		final int numCols_mlb_gameday_players = 13;	//this is a constant based on table and should not change
		//mlb_gameday_actions
		ArrayList<List<String>> curGame_mlb_gameday_actions;
		//this stores the current <action> tags awaiting the following <atbat> tag. This is necessary because there can be multiple <action> tags in a row before another <atbat> tag
		ArrayList<List<String>> curGame_mlb_gameday_actions_temp;
		List<String> curAction_mlb_gameday_actions = new ArrayList<String>();
		int numActions;
		final int numCols_mlb_gameday_actions = 19;	//this is a constant based on table and should not change
		//mlb_gameday_at_bats
		ArrayList<List<String>> curGame_mlb_gameday_at_bats;
		List<String> curAtBat_mlb_gameday_at_bats;
		int numAtBats;
		final int numCols_mlb_gameday_at_bats = 33;	//this is a constant based on table and should not change
		
		//variables to help parsing xml files
		//to help determine if we are searching home or away team currenly in xml
		String homeAway = "";
		//to determine if top of the inning or not
		String topBottom;
		//to determine if we need to grab <atbat.num> for mlb_gameday_actions table for next <atbat> tag. When an <action> tag is parsed, we need to do this
		boolean needAtBatNum;
		final int indexAtBatNum = 12;
		//to keep track of if current at bats are in top or bottom of inning in game_events xml for at_bats table
		boolean is_topHelper = false;
		//to keep track of starting pitcher ids, to compare to current pitcher ids to. Index = 0 for home, 1 for away
		String[] startPitID = new String[2];
		//to keep track of current field players ids
		//indexes are as follows: 0 = catcher, 1 = first, 2 = second, 3 = third, 4 = ss, 5 = lf, 6 = rf, 7 = cf
		String[][] curFieldPlayers = new String[2][8];
		int curFieldPlayersIndex = -1;	//0 for home team, 1 for away
		for(int i = 0; i < 8; i++){
			curFieldPlayers[0][i] = "";
			curFieldPlayers[1][i] = "";
		}
		//associate player names with ids. This is when there is an action tag (it only gives player names), that we can update the current fielders for at_bats table (which uses player ids)
		HashMap<String,String> playerNameIDs = new HashMap<String,String>();
		String curPlayerName = "";
		String curPlayerID = "";
		String curPlayerPos = "";
		String actionDesc;
		int pos1;
		int pos2;
		boolean defensiveSwitch;
		boolean cantFindPlayer;
		boolean cantFindPos;
			
		try {
			bwGames = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(gamesCSVFile), "UTF8"));
			bwPlayers = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(playersCSVFile), "UTF8"));
			bwActions = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(actionsCSVFile), "UTF8"));
			bwAtBats = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(atBatsCSVFile), "UTF8"));
			
			itr = gameIDsURLs.keySet().iterator();
			
			while(itr.hasNext()){
				curGID = (String) itr.next();
		
				gameXML = xmlFolder + curGID + "_game.xml";
				gameEventsXML = xmlFolder + curGID + "_game_events.xml";
				lineScoreXML = xmlFolder + curGID + "_linescore.xml";
				playersXML = xmlFolder + curGID + "_players.xml";
				playsXML = xmlFolder + curGID + "_plays.xml";
				
				gameXMLFile = new File(gameXML);
				gameEventsXMLFile = new File(gameEventsXML);
				lineScoreXMLFile = new File(lineScoreXML);
				playersXMLFile = new File(playersXML);
				playsXMLFile = new File(playsXML);
				
				if(!(gameXMLFile.exists() && gameEventsXMLFile.exists() && lineScoreXMLFile.exists() && gameXMLFile.exists() && playersXMLFile.exists() && playsXMLFile.exists())){
					errorLog.append("XML file missing for " + curGID + "\n");
				}else{
					//variables that need to be reset for each game
					curGame_mlb_gameday_games = new ArrayList<String>();
					curGame_mlb_gameday_players = new ArrayList<List<String>>();
					curGame_mlb_gameday_actions = new ArrayList<List<String>>();
					curGame_mlb_gameday_actions_temp = new ArrayList<List<String>>();
					curGame_mlb_gameday_at_bats = new ArrayList<List<String>>();
					
					//game
					fXmlFile = new File(gameXML);
					dbFactory = DocumentBuilderFactory.newInstance();
					dBuilder = dbFactory.newDocumentBuilder();
					docGame = dBuilder.parse(fXmlFile);
					
					//games_events
					fXmlFile = new File(gameEventsXML);
					dbFactory = DocumentBuilderFactory.newInstance();
					dBuilder = dbFactory.newDocumentBuilder();
					docGameEvents = dBuilder.parse(fXmlFile);
					
					//linescore
					fXmlFile = new File(lineScoreXML);
					dbFactory = DocumentBuilderFactory.newInstance();
					dBuilder = dbFactory.newDocumentBuilder();
					docLinescore = dBuilder.parse(fXmlFile);
						
					//players
					fXmlFile = new File(playersXML);
					dbFactory = DocumentBuilderFactory.newInstance();
					dBuilder = dbFactory.newDocumentBuilder();
					docPlayers = dBuilder.parse(fXmlFile);
						
					//plays
					fXmlFile = new File(playsXML);
					dbFactory = DocumentBuilderFactory.newInstance();
					dBuilder = dbFactory.newDocumentBuilder();
					docPlays = dBuilder.parse(fXmlFile);
					
					//optional, but recommended
					//read this - http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
					docGame.getDocumentElement().normalize();
					docGameEvents.getDocumentElement().normalize();
					docLinescore.getDocumentElement().normalize();
					docPlayers.getDocumentElement().normalize();
					docPlays.getDocumentElement().normalize();
				
					//plays XML
					//mlb_gameday_games.gid
					//mlb_gameday_players.game_id
					nList = docPlays.getElementsByTagName("game");
					if (nList.getLength() > 1){
						errorLog.append("More than one <game> tag in file " + playsXML + ".\n");
					}
					nNode = docPlays.getElementsByTagName("game").item(0);
					eElement = (Element) nNode;
					gid = eElement.getAttribute("id");
					game_id = eElement.getAttribute("id");
					//wind,temp and dome
					nList = docPlays.getElementsByTagName("weather");
					if (nList.getLength() > 1){
						errorLog.append("More than one <weather> tag in file " + playsXML + ".\n");
					}
					nNode = docPlays.getElementsByTagName("weather").item(0);
					eElement = (Element) nNode;
					temp = eElement.getAttribute("temp");
					dome = eElement.getAttribute("condition");
					wind = eElement.getAttribute("wind");
					
					//game XML
					//local_game_time,game_time_et
					nList = docGame.getElementsByTagName("game");
					if (nList.getLength() > 1){
						errorLog.append("More than one <game> tag in file " + gameXML + ".\n");
					}
					nNode = docGame.getElementsByTagName("game").item(0);
					eElement = (Element) nNode;
					local_game_time = eElement.getAttribute("local_game_time");
					game_time_et = eElement.getAttribute("game_time_et");
					game_type = eElement.getAttribute("type");
					//home_id, away_id
					nList = docGame.getElementsByTagName("team");
					for (int i = 0; i < nList.getLength(); i++){
						nNode = nList.item(i);
						eElement = (Element) nNode;
						if(eElement.getAttribute("type").equals("home")){
							home_id = eElement.getAttribute("code");
						}else if(eElement.getAttribute("type").equals("away")){
							away_id = eElement.getAttribute("code");
						}else{
							errorLog.append("<team> tag in file " + gameXML + " has type not equal to either \"home\" or \"away\".\n");
						}
					}
					//stadium_id,venue_w_chan_loc
					nList = docGame.getElementsByTagName("stadium");
					if (nList.getLength() > 1){
						errorLog.append("More than one <stadium> tag in file " + gameXML + ".\n");
					}
					nNode = docGame.getElementsByTagName("stadium").item(0);
					eElement = (Element) nNode;
					stadium_id = eElement.getAttribute("id");
					venue_w_chan_loc = eElement.getAttribute("venue_w_chan_loc");
				
					//players XML
					//umpire_hp_id,umpire_1b_id,umpire_2b_id,umpire_3b_id
					nList = docPlayers.getElementsByTagName("umpire");
					for (int i = 0; i < nList.getLength(); i++){
						nNode = nList.item(i);
						eElement = (Element) nNode;
						if(eElement.getAttribute("position").equals("home")){
							umpire_hp_id = eElement.getAttribute("id");
						}else if(eElement.getAttribute("position").equals("first")){
							umpire_1b_id = eElement.getAttribute("id");
						}else if(eElement.getAttribute("position").equals("second")){
							umpire_2b_id = eElement.getAttribute("id");
						}else if(eElement.getAttribute("position").equals("third")){
							umpire_3b_id = eElement.getAttribute("id");
						}else{
							errorLog.append("<umpire> tag in file " + playersXML + " has position not equal to either \"home\", \"first\", \"second\" or \"third\".\n");
						}
					}
					//home_dh_id,away_dh_id (mlb_gameday_games table)
					//grab all columns for mlb_gameday_players except game_id
					//grab the starting players for both teams (used for intial fielders for at_bats table)
					//initialize to nothing because no dh in national league
					home_dh_id = "";
					away_dh_id = "";
					nList = docPlayers.getElementsByTagName("team");
					for (int i = 0; i < nList.getLength(); i++){
						nNode = nList.item(i);
						eElement = (Element) nNode;
						homeAway = eElement.getAttribute("type");
						if (homeAway.equals("home")){
							curFieldPlayersIndex = 0;
						}else if (homeAway.equals("away")){
							curFieldPlayersIndex = 1;
						}else{
							curFieldPlayersIndex = -1;	//set to -1 to not cause run code to set initial field player
							errorLog.append("\"type\" attribute in <player> tag has a value other than \"home\" or \"away\".\n");
						}
						if(nNode.hasChildNodes()){
							nList2 = nNode.getChildNodes();
							for (int j = 0; j < nList2.getLength(); j++){
								nNode = nList2.item(j);
								if(nNode.getNodeType() == Node.ELEMENT_NODE){
									eElement = (Element) nNode;
									if(eElement.getTagName().equals("player")){
										if(eElement.getAttribute("game_position").equals("DH")){
											if (homeAway.equals("home")){
												home_dh_id = eElement.getAttribute("id");
											}else if (homeAway.equals("away")){
												away_dh_id = eElement.getAttribute("id");
											}else{
												errorLog.append("\"type\" attribute in <player> tag has a value other than \"home\" or \"away\".\n");
											}
										}
										//set the initial field players
										if(curFieldPlayersIndex == 0 || curFieldPlayersIndex == 1){
											if(eElement.getAttribute("game_position").equals("P")){
												startPitID[curFieldPlayersIndex] = eElement.getAttribute("id");
											}else if(eElement.getAttribute("game_position").equals("C")){
												curFieldPlayers[curFieldPlayersIndex][0] = eElement.getAttribute("id");
											}else if(eElement.getAttribute("game_position").equals("1B")){
												curFieldPlayers[curFieldPlayersIndex][1] = eElement.getAttribute("id");
											}else if(eElement.getAttribute("game_position").equals("2B")){
												curFieldPlayers[curFieldPlayersIndex][2] = eElement.getAttribute("id");
											}else if(eElement.getAttribute("game_position").equals("3B")){
												curFieldPlayers[curFieldPlayersIndex][3] = eElement.getAttribute("id");
											}else if(eElement.getAttribute("game_position").equals("SS")){
												curFieldPlayers[curFieldPlayersIndex][4] = eElement.getAttribute("id");
											}else if(eElement.getAttribute("game_position").equals("LF")){
												curFieldPlayers[curFieldPlayersIndex][5] = eElement.getAttribute("id");
											}else if(eElement.getAttribute("game_position").equals("RF")){
												curFieldPlayers[curFieldPlayersIndex][6] = eElement.getAttribute("id");
											}else if(eElement.getAttribute("game_position").equals("CF")){
												curFieldPlayers[curFieldPlayersIndex][7] = eElement.getAttribute("id");
											}
										}
											
										//keep track of all player names and their id's for at_bat table later (keeps track of current fielders)
										curPlayerName = eElement.getAttribute("first") + " " + eElement.getAttribute("last");
										curPlayerID = eElement.getAttribute("id");
										playerNameIDs.put(curPlayerName,curPlayerID);
										
										//mlb_gameday_players
										gameday_id = eElement.getAttribute("id");
										first = eElement.getAttribute("first");
										last = eElement.getAttribute("last");
										number = eElement.getAttribute("num"); 
										boxname = eElement.getAttribute("boxname");
										position = eElement.getAttribute("position"); 
										current_position = eElement.getAttribute("current_position"); 
										strThrows = eElement.getAttribute("rl"); 
										status = eElement.getAttribute("status"); 
										team_id = eElement.getAttribute("team_id");   
										bats = eElement.getAttribute("bats"); 
										
										//add row to mlb_gameday_players table
										curPlayer_mlb_gameday_players = new ArrayList<String>();
										curPlayer_mlb_gameday_players.add("");	//for autoincrement field
										curPlayer_mlb_gameday_players.add(gameday_id);
										curPlayer_mlb_gameday_players.add(first);
										curPlayer_mlb_gameday_players.add(last);
										curPlayer_mlb_gameday_players.add(number);
										curPlayer_mlb_gameday_players.add(boxname);
										curPlayer_mlb_gameday_players.add(position);
										curPlayer_mlb_gameday_players.add(current_position);
										curPlayer_mlb_gameday_players.add(strThrows);
										curPlayer_mlb_gameday_players.add(status);
										curPlayer_mlb_gameday_players.add(team_id);
										curPlayer_mlb_gameday_players.add(game_id);	//this value is taken from plays xml, so that file needs to be parsed before players xml file
										curPlayer_mlb_gameday_players.add(bats);
										curGame_mlb_gameday_players.add(curPlayer_mlb_gameday_players);
									}
								}
							}
						}else{
							errorLog.append("No child nodes for <team> tag, attribute type = \"" + homeAway + "\" in  " + playersXML + " .\n");
						}
					}
					
					//linescore XML
					//league_id,sport_code,league
					nList = docLinescore.getElementsByTagName("game");
					if (nList.getLength() > 1){
						errorLog.append("More than one <game> tag in file " + lineScoreXML + ".\n");
					}
					nNode = docLinescore.getElementsByTagName("game").item(0);
					eElement = (Element) nNode;
					league_id = eElement.getAttribute("home_league_id");
					sport_code = eElement.getAttribute("home_sport_code");
					league = eElement.getAttribute("league");
					
					//game events xml
					needAtBatNum = false;
					nList = docGameEvents.getElementsByTagName("inning");
					for (int i = 0; i < nList.getLength(); i++){
						nNode = nList.item(i);
						eElement = (Element) nNode;
						inning = eElement.getAttribute("num");
						nList2 = nNode.getChildNodes();
						for(int j = 0; j < nList2.getLength(); j++){
							nNode = nList2.item(j);
							if(nNode.getNodeType() == Node.ELEMENT_NODE){
								eElement = (Element) nNode;
								topBottom = eElement.getTagName();
								if(topBottom.equals("top")){
									is_top = "1";
									curFieldPlayersIndex = 0;
								}else if(topBottom.equals("bottom")){
									is_top = "0";
									curFieldPlayersIndex = 1;
								}else{
									errorLog.append("File " + docGameEvents + " inning = " + inning + " has a top/bottom tag = " + topBottom + "\n");
								}
								if(topBottom.equals("top") || topBottom.equals("bottom")){
									nList3 = nNode.getChildNodes();
									for (int k = 0; k < nList3.getLength(); k++){
										nNode = nList3.item(k);
										if(nNode.getNodeType() == Node.ELEMENT_NODE){
											eElement = (Element) nNode;
											if(eElement.getTagName().equals("action")){
												needAtBatNum = true;
												
												//get defensive substitutions (for at_bat table)
												actionDesc = eElement.getAttribute("des");
												pos1 = 0;
												pos2 = 0;
												defensiveSwitch = false;
												cantFindPlayer = false;
												cantFindPos = false;
												curPlayerName = "";
												curPlayerID = "";
												curPlayerPos = "";
												if(actionDesc.contains(" remains in the game as the ")){
													defensiveSwitch = true;
													//get player id
													if(actionDesc.startsWith("Defensive Substitution: ")){
														pos1 = 24;
													}else{
														pos1 = 0;
													}
													pos2 = actionDesc.indexOf(" remains in the game as the ");
													curPlayerName = actionDesc.substring(pos1,pos2);
													if(playerNameIDs.containsKey(curPlayerName)){
														curPlayerID = playerNameIDs.get(curPlayerName);
													}else{
														cantFindPlayer = true;
													}
													//get player pos
													pos1 = pos2 + 28;
													if(actionDesc.indexOf(",",pos1) > 0){
														pos2 = actionDesc.indexOf(",",pos1);	
													}else{
														pos2 = actionDesc.indexOf(".",pos1);
													}
													curPlayerPos = actionDesc.substring(pos1,pos2);
												}else if(actionDesc.startsWith("Defensive Substitution: ") && actionDesc.contains(" replaces ")){
													defensiveSwitch = true;
													//get player id
													pos1 = 24;
													pos2 = actionDesc.indexOf(" replaces ");
													curPlayerName = actionDesc.substring(pos1,pos2);
													if(playerNameIDs.containsKey(curPlayerName)){
														curPlayerID = playerNameIDs.get(curPlayerName);
													}else{
														cantFindPlayer = true;
													}
													//get player pos
													pos1 = actionDesc.indexOf(", playing ") + 10;
													pos2 = actionDesc.indexOf(".",pos1);
													curPlayerPos = actionDesc.substring(pos1,pos2);
												}else if(actionDesc.contains("Defensive switch from ")){
													defensiveSwitch = true;
													//get player pos
													pos1 = actionDesc.indexOf(" to ") + 4;
													pos2 = actionDesc.indexOf(" for ");
													curPlayerPos = actionDesc.substring(pos1,pos2);
													//get player id
													pos1 = pos2 + 5;
													pos2 = actionDesc.lastIndexOf(".");
													//pos2 = actionDesc.length()-1;	//instead of looking for period, just take one character off end of string. this is because there might be player names that contain a period
													curPlayerName = actionDesc.substring(pos1,pos2);
													if(playerNameIDs.containsKey(curPlayerName)){
														curPlayerID = playerNameIDs.get(curPlayerName);
													}else{
														cantFindPlayer = true;
													}
												}
				
												if(defensiveSwitch){
													if(cantFindPlayer){
														errorLog.append("File: " + gameEventsXML + "\tCan't find player name = " + curPlayerName + "\n");
													}else{
														//assuming that <action> tags in top of the inning are for home team and bottom of the inning are for away team
														if(topBottom.equals("top")){
															curFieldPlayersIndex = 0;
														}else{
															curFieldPlayersIndex = 1;
														}
														if(curPlayerPos.toLowerCase().contains("catcher")){
															curFieldPlayers[curFieldPlayersIndex][0] = curPlayerID;
														}else if(curPlayerPos.toLowerCase().contains("first base")){
															curFieldPlayers[curFieldPlayersIndex][1] = curPlayerID;
														}else if(curPlayerPos.toLowerCase().contains("second base")){
															curFieldPlayers[curFieldPlayersIndex][2] = curPlayerID;
														}else if(curPlayerPos.toLowerCase().contains("third base")){
															curFieldPlayers[curFieldPlayersIndex][3] = curPlayerID;
														}else if(curPlayerPos.toLowerCase().contains("shortstop")){
															curFieldPlayers[curFieldPlayersIndex][4] = curPlayerID;
														}else if(curPlayerPos.toLowerCase().contains("left field")){
															curFieldPlayers[curFieldPlayersIndex][5] = curPlayerID;
														}else if(curPlayerPos.toLowerCase().contains("right field")){
															curFieldPlayers[curFieldPlayersIndex][6] = curPlayerID;
														}else if(curPlayerPos.toLowerCase().contains("center field")){
															curFieldPlayers[curFieldPlayersIndex][7] = curPlayerID;
														}else if(curPlayerPos.toLowerCase().contains("designated hitter")){
															//don't do anything when someone becomes dh currently
														}else{
															cantFindPos = true;
															errorLog.append("File: " + gameEventsXML + "\tCan't find player position = " + curPlayerPos + "\n");
														}
													}
												}
														
												
												balls = eElement.getAttribute("b");
												strikes = eElement.getAttribute("s");
												outs = eElement.getAttribute("o");
												des = eElement.getAttribute("des");
												strEvent = eElement.getAttribute("event");
												player_id = eElement.getAttribute("player");	
												pitch_number = eElement.getAttribute("pitch");
												event_num = eElement.getAttribute("event_num");
												tfs_zulu = eElement.getAttribute("tfs_zulu");
												
												//fix because event_num is not null in sql, but it is also not in every xml file
												//it is being stored as a blank value, and then erroring when csv is bulk inserted to sql
												if(event_num.equals("")){
													//event_num = "-1";
												}
												
												//add row to mlb_gameday_players table
												curAction_mlb_gameday_actions = new ArrayList<String>();
												curAction_mlb_gameday_actions.add("");	//for autoincrement field
												curAction_mlb_gameday_actions.add(balls);
												curAction_mlb_gameday_actions.add(strikes);
												curAction_mlb_gameday_actions.add(outs);
												curAction_mlb_gameday_actions.add(des);
												curAction_mlb_gameday_actions.add(strEvent);
												curAction_mlb_gameday_actions.add(player_id);
												curAction_mlb_gameday_actions.add(pitch_number);
												curAction_mlb_gameday_actions.add("");
												curAction_mlb_gameday_actions.add("");
												curAction_mlb_gameday_actions.add(game_id);	//this value is taken from plays xml, so that file needs to be parsed before game_events xml file
												curAction_mlb_gameday_actions.add(event_num);
												//at_bat_num will be inserted here once next <atbat> tag is parsed, shifting indices after this index
												curAction_mlb_gameday_actions.add("");
												curAction_mlb_gameday_actions.add("");
												curAction_mlb_gameday_actions.add("");
												curAction_mlb_gameday_actions.add("");
												curAction_mlb_gameday_actions.add("");
												curAction_mlb_gameday_actions.add(tfs_zulu);
												curGame_mlb_gameday_actions_temp.add(curAction_mlb_gameday_actions);
											}else if(eElement.getTagName().equals("atbat")){
												//do something with atbat tag (for now just grabbing atbat.num if we need to for actions table)
												if(needAtBatNum){
													at_bat_num = eElement.getAttribute("num");
													numActions = curGame_mlb_gameday_actions_temp.size();
													if(numActions == 0){
														errorLog.append("needAtBatNum was set to true, but there are no actions added to curGame_mlb_gameday_actions_temp. File " + docGameEvents + "\n");
													}
													for(int l = 0; l < numActions; l++){
														curAction_mlb_gameday_actions = curGame_mlb_gameday_actions_temp.get(l);
														curAction_mlb_gameday_actions.add(indexAtBatNum,at_bat_num);
														curGame_mlb_gameday_actions.add(curAction_mlb_gameday_actions);
													}
													//reset variables
													needAtBatNum = false;	
													curGame_mlb_gameday_actions_temp = new ArrayList<List<String>>();
												}
												
												//add current at_bat to at_bat table
												num = eElement.getAttribute("num");
												ball = eElement.getAttribute("b");
												strike = eElement.getAttribute("s");
												outs = eElement.getAttribute("o");
												batter_id = eElement.getAttribute("batter");
												pitcher_id = eElement.getAttribute("pitcher"); 
												des = eElement.getAttribute("des");
												eventAtBat = eElement.getAttribute("event");
												on_first = eElement.getAttribute("b1");
												if(!on_first.matches("^\\s*\\d+\\s*$")){
													on_first = "";
												}
												on_second = eElement.getAttribute("b2");
												if(!on_second.matches("^\\s*\\d+\\s*$")){
													on_second = "";
												}
												on_third = eElement.getAttribute("b3");
												if(!on_third.matches("^\\s*\\d+\\s*$")){
													on_third = "";
												}
												if(pitcher_id.equals(startPitID[curFieldPlayersIndex])){
													pitcher_role = "s";
												}else{
													pitcher_role = "r";
												}	
												
												curAtBat_mlb_gameday_at_bats = new ArrayList<String>();
												curAtBat_mlb_gameday_at_bats.add("");
												curAtBat_mlb_gameday_at_bats.add(game_id);	//this value is taken from plays xml, so that file needs to be parsed before game_events xml file
												curAtBat_mlb_gameday_at_bats.add(inning);
												curAtBat_mlb_gameday_at_bats.add(num);
												curAtBat_mlb_gameday_at_bats.add(ball);
												curAtBat_mlb_gameday_at_bats.add(strike);
												curAtBat_mlb_gameday_at_bats.add(outs);
												curAtBat_mlb_gameday_at_bats.add(batter_id);
												curAtBat_mlb_gameday_at_bats.add(pitcher_id);
												curAtBat_mlb_gameday_at_bats.add("");
												curAtBat_mlb_gameday_at_bats.add(des);
												curAtBat_mlb_gameday_at_bats.add(eventAtBat);
												curAtBat_mlb_gameday_at_bats.add("");
												curAtBat_mlb_gameday_at_bats.add(on_first);
												curAtBat_mlb_gameday_at_bats.add(on_second);
												curAtBat_mlb_gameday_at_bats.add(on_third);
												curAtBat_mlb_gameday_at_bats.add(pitcher_role);
												curAtBat_mlb_gameday_at_bats.add("");
												curAtBat_mlb_gameday_at_bats.add("");
												curAtBat_mlb_gameday_at_bats.add("");
												curAtBat_mlb_gameday_at_bats.add(curFieldPlayers[curFieldPlayersIndex][0]);
												curAtBat_mlb_gameday_at_bats.add(is_top);
												curAtBat_mlb_gameday_at_bats.add("");
												curAtBat_mlb_gameday_at_bats.add("");
												for(int l = 1; l < 8; l++){
													curAtBat_mlb_gameday_at_bats.add(curFieldPlayers[curFieldPlayersIndex][l]);
												}
												curAtBat_mlb_gameday_at_bats.add("");
												curAtBat_mlb_gameday_at_bats.add("");
												
												curGame_mlb_gameday_at_bats.add(curAtBat_mlb_gameday_at_bats);
											}
										}
									}
								}
							}
						}
					}
					
					numActions = curGame_mlb_gameday_actions_temp.size();
					if(numActions != 0){
						errorLog.append(numActions + " <action> tags were not saved to csv/sent to sql for file " + docGameEvents + "\n");
					}
					
					
					//storing mlb_gameday_games info in arraylist for consistency (mainly to check for delimiter in any words added to csv)
					curGame_mlb_gameday_games.add("");
					curGame_mlb_gameday_games.add(gid);
					curGame_mlb_gameday_games.add("");
					curGame_mlb_gameday_games.add(home_id);
					curGame_mlb_gameday_games.add(away_id);
					curGame_mlb_gameday_games.add("");
					curGame_mlb_gameday_games.add(umpire_hp_id);
					curGame_mlb_gameday_games.add(umpire_1b_id);
					curGame_mlb_gameday_games.add(umpire_2b_id);
					curGame_mlb_gameday_games.add(umpire_3b_id);
					curGame_mlb_gameday_games.add(wind);
					curGame_mlb_gameday_games.add("");
					curGame_mlb_gameday_games.add("");
					curGame_mlb_gameday_games.add(temp);
					curGame_mlb_gameday_games.add(game_type);
					curGame_mlb_gameday_games.add(stadium_id);
					curGame_mlb_gameday_games.add(dome);
					curGame_mlb_gameday_games.add("");
					curGame_mlb_gameday_games.add("");
					curGame_mlb_gameday_games.add(sport_code);
					curGame_mlb_gameday_games.add(league);
					curGame_mlb_gameday_games.add(league_id);
					curGame_mlb_gameday_games.add(home_dh_id);
					curGame_mlb_gameday_games.add(away_dh_id);
					
					//write current game to file
					//mlb_gameday_at_bats
					numAtBats = curGame_mlb_gameday_at_bats.size();
					for(int i = 0; i < numAtBats; i++){
						curAtBat_mlb_gameday_at_bats = curGame_mlb_gameday_at_bats.get(i);
						for(int j = 0; j < numCols_mlb_gameday_at_bats; j++){
							curWord = curAtBat_mlb_gameday_at_bats.get(j);
							if(curWord.contains(MLBAMdelimiter)){
								errorLog.append("MLBAMdelimiter = " + MLBAMdelimiter + " found in word " + curWord + " for table mlb_gameday_at_bats, row " + (i+1) + " column " + (j+1) + " gid = " + curGID + "\n");
								curWord = curWord.replace(MLBAMdelimiter,"");
							}
							bwAtBats.write(curWord);
							if(j < numCols_mlb_gameday_at_bats-1){
								bwAtBats.write(MLBAMdelimiter);
							}
						}
						bwAtBats.newLine();
					}
					//mlb_gameday_actions
					numActions = curGame_mlb_gameday_actions.size();
					for(int i = 0; i < numActions; i++){
						curAction_mlb_gameday_actions = curGame_mlb_gameday_actions.get(i);
						if(curAction_mlb_gameday_actions.size() != numCols_mlb_gameday_actions){
							errorLog.append("Row number = " + i + " for game_id = " + game_id + " for mlb_gameday_actions table does not have the correct number of columns and so it was not saved to csv (or sent to sql)");
						}else{
							for(int j = 0; j < numCols_mlb_gameday_actions; j++){
								curWord = curAction_mlb_gameday_actions.get(j);
								if(curWord.contains(MLBAMdelimiter)){
									errorLog.append("MLBAMdelimiter = " + MLBAMdelimiter + " found in word " + curWord + " for table mlb_gameday_actions, row " + (i+1) + " column " + (j+1) + " gid = " + curGID + "\n");
									curWord = curWord.replace(MLBAMdelimiter,"");
								}
								bwActions.write(curWord);
								if(j < numCols_mlb_gameday_actions-1){
									bwActions.write(MLBAMdelimiter);
								}
							}
							bwActions.newLine();
						}
					}
					//bwGames.write(MLBAMdelimiter + gid + MLBAMdelimiter + MLBAMdelimiter + home_id + MLBAMdelimiter + away_id + MLBAMdelimiter + MLBAMdelimiter + umpire_hp_id + MLBAMdelimiter + umpire_1b_id + MLBAMdelimiter + umpire_2b_id + MLBAMdelimiter + umpire_3b_id + MLBAMdelimiter + wind + MLBAMdelimiter + MLBAMdelimiter + MLBAMdelimiter + temp + MLBAMdelimiter + game_type + MLBAMdelimiter + stadium_id + MLBAMdelimiter + dome + MLBAMdelimiter + MLBAMdelimiter + MLBAMdelimiter + sport_code + MLBAMdelimiter + league + MLBAMdelimiter + league_id + MLBAMdelimiter + home_dh_id + MLBAMdelimiter + away_dh_id);
					//bwGames.newLine();
					//mlb_gameday_games
					for(int i = 0; i < numCols_mlb_gameday_games; i++){
						curWord = curGame_mlb_gameday_games.get(i);
						if(curWord.contains(MLBAMdelimiter)){
							errorLog.append("MLBAMdelimiter = " + MLBAMdelimiter + " found in word " + curWord + " for table mlb_gameday_games, column " + (i+1) + " gid = " + curGID + "\n");
							curWord = curWord.replace(MLBAMdelimiter,"");
						}
						bwGames.write(curWord);
						if(i < numCols_mlb_gameday_games-1){
							bwGames.write(MLBAMdelimiter);
						}
					}	
					bwGames.newLine();
					//mlb_gameday_players
					numPlayers = curGame_mlb_gameday_players.size();
					for(int i = 0; i < numPlayers; i++){
						curPlayer_mlb_gameday_players = curGame_mlb_gameday_players.get(i);
						if(curPlayer_mlb_gameday_players.size() != numCols_mlb_gameday_players){
							errorLog.append("Row number = " + i + " for game_id = " + game_id + " for mlb_gameday_players table does not have the correct number of columns and so it was not saved to csv (or sent to sql)" + "\n");
						}else{
							for(int j = 0; j < numCols_mlb_gameday_players; j++){
								curWord = curPlayer_mlb_gameday_players.get(j);
								if(curWord.contains(MLBAMdelimiter)){
									errorLog.append("MLBAMdelimiter = " + MLBAMdelimiter + " found in word " + curWord + " for table mlb_gameday_players, row " + (i+1) + " column " + (j+1) + " gid = " + curGID + "\n");
									curWord = curWord.replace(MLBAMdelimiter,"");
								}
								bwPlayers.write(curWord);
								if(j < numCols_mlb_gameday_players-1){
									bwPlayers.write(MLBAMdelimiter);
								}
							}
							bwPlayers.newLine();
						}
					}
				}
			}
			
			bwAtBats.close();
			bwActions.close();			
			bwGames.close();
			bwPlayers.close();
			
	    } catch (Exception e) {
			e.printStackTrace();
	    }
	    
	    if(errorLog.toString().equals("")){
		    returnValue = 0;
		    errorLog.append("createMLBAMcsv() completed successfully!");
	    }
	    return returnValue;
	}
	
	public void downloadXMLfile(String url, String filePath, boolean overwrite){
		//variables for downloading xml files
		Downloader downloader = new Downloader();
		double lambda = .9;
		File file;
		
		file = new File(filePath);
		if(overwrite){
			downloader.downloadPage(url,true,true,filePath,true,lambda,true);
		}else{
			if(file.exists() && file.isFile()){
				//don't do anything, because overwrite is set to false and file already exists
			}else{
				downloader.downloadPage(url,true,true,filePath,true,lambda,true);
			}
		}
	}
}