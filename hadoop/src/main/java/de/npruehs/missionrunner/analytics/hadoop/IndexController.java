package de.npruehs.missionrunner.analytics.hadoop;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.reactive.function.client.WebClient;

import de.npruehs.missionrunner.analytics.hadoop.model.ListStatusResponse;

@Controller
public class IndexController {
	@GetMapping("/")
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

		return "index";
	}
}
