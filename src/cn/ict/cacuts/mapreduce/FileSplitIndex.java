package cn.ict.cacuts.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;

public class FileSplitIndex {
	  private Path file;
	  private long start;
	  private long length;
	  private String[] hosts;
	  public FileSplitIndex() {}

	  /** Constructs a split index with host information
	   *
	   * @param file the file name
	   * @param start the position of the first byte in the file to process
	   * @param length the number of bytes in the file to process
	   * @param hosts the list of hosts containing the block, possibly null
	   */
	  public FileSplitIndex(Path file, long start, long length, String[] hosts) {
	    this.file = file;
	    this.start = start;
	    this.length = length;
	    this.hosts = hosts;
	  }
	  /** The file containing this split's data. */
	  public Path getPath() { return file; }
	  
	  /** The position of the first byte in the file to process. */
	  public long getStart() { return start; }
	  
	  /** The number of bytes in the file to process. */
	  public long getLength() { return length; }

	  @Override
	  public String toString() {
		  StringBuffer buf = new StringBuffer();
		  buf.append("file : " + file + "\n");
		  buf.append("start-offset : " + start + "\n");
		  buf.append("length : " + length + "\n");
		  buf.append("locations : " + "\n");
		  for (String loc: hosts) {
			  buf.append("  " + loc + "\n");
		  }
		  return buf.toString();
	  }
	  public void write(DataOutput out) throws IOException{
			WritableUtils.writeString(out,file.toString());
			WritableUtils.writeVLong(out, start);
			WritableUtils.writeVLong(out, length);
			WritableUtils.writeVInt(out, hosts.length);
		      for (int i = 0; i < hosts.length; i++) {
		        Text.writeString(out, hosts[i]);
		      }
	  }

	public void readFields(DataInput in) throws IOException {
		file = new Path(WritableUtils.readString(in));
		start = WritableUtils.readVLong(in);
		length = WritableUtils.readVLong(in);
		hosts = new String[WritableUtils.readVInt(in)];
		for (int i = 0; i < hosts.length; i++) {
			hosts[i] = new String(Text.readString(in));
		}
	}
}
