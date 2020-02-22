package de.npruehs.missionrunner.analytics.hadoop;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.reactive.function.client.WebClient;

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
