package cn.ict.cacuts.mapreduce.compiler;

public class Channel {
	private TransmitType trtype = TransmitType.localFile;
	private TaskStruct from;
	private TaskStruct to;
	private int outputIndex;
	private int inputIndex;
	public Channel(TaskStruct from, int outputIndex, TaskStruct to, int inputIndex){
		this.from = from;
		this.to  = to;
		this.inputIndex = inputIndex;
		this.outputIndex = outputIndex;
	}
	public Channel(TaskStruct from, int outputIndex, TaskStruct to, int inputIndex, TransmitType trtype){
		this.from = from;
		this.to  = to;
		this.inputIndex = inputIndex;/////////////////***what is this inputIndex for???????
		this.outputIndex = outputIndex;
		this.trtype = trtype;
	}
	public TaskStruct getFrom(){
		return this.from;
	}
	public TaskStruct getTo(){
		return this.to;
	}
	public int getOutputIndex(){
		return this.outputIndex;
	}
	public int getInputIndex(){
		return this.inputIndex;
	}
}
