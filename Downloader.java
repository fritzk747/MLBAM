import java.util.Random;
import java.io.*;
import java.awt.Robot;
import java.awt.event.InputEvent;
import java.awt.event.KeyEvent;
import java.awt.Desktop;
import java.net.URL;

public class Downloader{
	public Downloader(){
	}
	
	//using a poisson distribution to generate waiting times
	//lambda = .1 results in average 10 seconds per download
	public void downloadPage(String url, boolean useUserAgent, String userAgent, boolean specifyLocation, String fileLocation, boolean pause, double lambda, boolean verbose){
		String command;
		Random rng = new Random();
		double randomNumber;
		int numMilliseconds;
		
		randomNumber = Math.log(rng.nextDouble()) * -1 / lambda + 1;
		numMilliseconds = (int)(randomNumber);
		numMilliseconds = numMilliseconds * 1000;
		command = "wget ";
		
		if (useUserAgent){
			command = command + userAgent + " ";
		}
		if(specifyLocation){
			command = command + "-O \"" + fileLocation + "\" ";
		}
		
		command = command + url;
		if(verbose){
			System.out.println(command);
		}
		try
		{	
			Process process = Runtime.getRuntime().exec(command);
		}catch (IOException e){
		    e.printStackTrace();
		}
		
		if(pause){
			try{
				Thread.sleep(numMilliseconds);
			}catch(Exception e){
			}
		}
	}
	
	//using a poisson distribution to generate waiting times
	//lambda = .1 results in average 10 seconds per download
	public void downloadPage(String url, boolean useUserAgent, boolean specifyLocation, String fileLocation, boolean pause, double lambda, boolean verbose){
		String userAgent = "--user-agent=\"Mozilla/5.0 (Windows NT 5.2; rv:2.0.1) Gecko/20100101 Firefox/4.0.1/\"";
	
		downloadPage(url, useUserAgent, userAgent, specifyLocation, fileLocation, pause, lambda, verbose);
	}
		
}