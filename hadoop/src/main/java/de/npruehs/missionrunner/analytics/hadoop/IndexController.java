package de.npruehs.missionrunner.analytics.hadoop;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.reactive.function.client.WebClient;

import de.npruehs.missionrunner.analytics.hadoop.model.AnalyticsEventWithCount;
import de.npruehs.missionrunner.analytics.hadoop.model.FileToProcess;
import de.npruehs.missionrunner.analytics.hadoop.model.ListStatusResponse;

@Controller
public class IndexController {
	@Autowired
	private AnalyticsFileProcessor processor;
	
	@GetMapping("/")
	public String index() {
		return "redirect:index";
	}
	
	@GetMapping("/index")
	public String index(Model model) {
		ListStatusResponse inputFolderContents = WebClient.create("http://localhost:9870")
	        .get()
	        .uri("/webhdfs/v1/user/npruehs/input?user.name=npruehs&op=LISTSTATUS")
	        .retrieve()
	        .bodyToMono(ListStatusResponse.class)
	        .block();

		ListStatusResponse outputFolderContents = WebClient.create("http://localhost:9870")
		        .get()
		        .uri("/webhdfs/v1/user/npruehs/output?user.name=npruehs&op=LISTSTATUS")
		        .retrieve()
		        .bodyToMono(ListStatusResponse.class)
		        .block();

        if (inputFolderContents != null &&
        		inputFolderContents.getFileStatuses() != null &&
        				inputFolderContents.getFileStatuses().getFileStatus() != null) {
        	model.addAttribute("inputFiles", inputFolderContents.getFileStatuses().getFileStatus());
        }
        
        if (outputFolderContents != null &&
        		outputFolderContents.getFileStatuses() != null &&
        				outputFolderContents.getFileStatuses().getFileStatus() != null) {
        	model.addAttribute("outputFiles", outputFolderContents.getFileStatuses().getFileStatus());
        }
        
        model.addAttribute("fileToProcess", new FileToProcess());
        
		return "index";
	}
	
	@GetMapping("/get/{fileName}")
	public String get(Model model, @PathVariable("fileName") String fileName) throws MalformedURLException, IOException {
		BufferedInputStream in = new BufferedInputStream(
				new URL("http://localhost:9870/webhdfs/v1/user/npruehs/output/" + fileName + "/part-r-00000?user.name=npruehs&op=OPEN")
				.openStream());
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		  
	    byte dataBuffer[] = new byte[1024];
	    int bytesRead;
	    while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -1) {
	    	out.write(dataBuffer, 0, bytesRead);
	    }
	
	    // Parse data.
	    ArrayList<AnalyticsEventWithCount> eventsWithCount = new ArrayList<AnalyticsEventWithCount>();
	    
	    String rawEvents = out.toString();
	    String[] events = rawEvents.split("\n");
	    
	    for (String rawEventWithCount : events) {
	    	String[] splitEventWithCount = rawEventWithCount.split("\t");
	    	
	    	AnalyticsEventWithCount eventWithCount = new AnalyticsEventWithCount();
	    	eventWithCount.setEventId(splitEventWithCount[0]);
	    	eventWithCount.setCount(Integer.parseInt(splitEventWithCount[1]));
	    	
	    	eventsWithCount.add(eventWithCount);
	    }
	    
		model.addAttribute("fileName", fileName);
        model.addAttribute("eventsWithCount", eventsWithCount);
        
		return "results";
	}
	
	@PostMapping("/process")
	public String processProcess(Model model, @ModelAttribute FileToProcess fileToProcess) {
		try {
			if (!processor.process(fileToProcess.getFileName())) {
				model.addAttribute("errorMessage", "Processing failed.");
				return "error";
			}
		} catch (Exception e) {
			model.addAttribute("errorMessage", e.getMessage());
			return "error";
		}
		
	    return "redirect:index";
	}
}
