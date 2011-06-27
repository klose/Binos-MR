package cn.ict.cacuts.mapreduce.compiler;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Iterator;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * @className CreateJobXml.java
 * @author Yuanzheng Lu
 * @description the class is used for creating task xml files and test the robust of it.
 * @date 2011-03-21
*/
public class CreateTaskXml {
		private TaskStruct taskStruct;
		private Document document;
		private String filename;
		
		/*
		 * @param  JobStruct the job which will be created xml information.
		 * @param  map the task lists corresponding to the job
		 * */
		public CreateTaskXml(TaskStruct ts) throws ParserConfigurationException{
			this.taskStruct = ts;
			this.filename = this.taskStruct.getLocalXMLPath();
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			document = builder.newDocument();
		}
		
		/*
		 * @description the function is used for creating the body of job.xml file.
		 * */
		private void toWrite(){
			Element root = document.createElement("task");	
			document.appendChild(root);
			Element taskId = document.createElement("taskId");
			taskId.appendChild(document.createTextNode(taskStruct.getTaskId()));
			root.appendChild(taskId);
			
			
			Element taskJarPath = document.createElement("jarPath");
			taskJarPath.appendChild(document.createTextNode(taskStruct.getTaskJarPath()));
			root.appendChild(taskJarPath);
			
			Element taskOperationClass = document.createElement("operationClass");
			taskOperationClass.appendChild(document.createTextNode(taskStruct.getClassName()));
			root.appendChild(taskOperationClass);
			
			Element inputPath = document.createElement("inputPath");
			inputPath.setAttribute("inputPathNum", String.valueOf(taskStruct.getInputPath().length));
			int i = 0;
			for(i = 0;i < taskStruct.getInputPath().length;i++){
				Element path = document.createElement("path");
				path.setAttribute("id", String.valueOf(i+1));
				path.appendChild(document.createTextNode(taskStruct.getInputPath()[i]));
				inputPath.appendChild(path);
			}
			root.appendChild(inputPath);
			Element outputPath = document.createElement("outputPath");
			outputPath.setAttribute("outputPathNum", String.valueOf(taskStruct.getOutputPath().length));
			for(i=0;i<taskStruct.getOutputPath().length;i++){
				Element path = document.createElement("path");
				path.setAttribute("id", String.valueOf(i+1));
				path.appendChild(document.createTextNode(taskStruct.getOutputPath()[i]));
				outputPath.appendChild(path);
			}
			root.appendChild(outputPath);							
		}
		
		/*
		 * @description  save the produced file to the indicated path ,which is this.file
		 * */
		private void toSave(){
			try{
				TransformerFactory tf=TransformerFactory.newInstance();
				Transformer transformer = tf.newTransformer();
				DOMSource source = new DOMSource(document);
				transformer.setOutputProperty(OutputKeys.ENCODING, "GB2312");
				transformer.setOutputProperty(OutputKeys.INDENT, "yes");
				PrintWriter pw = new PrintWriter(new FileOutputStream(filename));
				StreamResult result = new StreamResult(pw);
				transformer.transform(source, result);
			}catch(TransformerException mye){
				mye.printStackTrace();
			}catch(IOException exp){
				exp.printStackTrace();
			}
		}
		/*
		 * @param filePath the xml path.
		 * check the file exists or not before creating it
		 * */
		public static void checkFileExist(String filePath){
			File file = new File(filePath);
			String path = file.getParentFile().getAbsolutePath();
			File fileath = new File(path);
			if(!fileath.exists()){
				fileath.mkdirs();
				try {
					file.createNewFile();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		/*
		 * @description the interface for other models 
		 * */
		public void createTaskXml(){
			checkFileExist(this.taskStruct.getLocalXMLPath());
			toWrite();
			toSave();			
		}
}
