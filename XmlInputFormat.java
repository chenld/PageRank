package edu.uci.inforet;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;


/**
* Reads records that are delimited by a specific begin/end tag.
*/
public class XmlInputFormat extends TextInputFormat {
  
  public static final String PAGE_START_TAG = "<page>";
  public static final String PAGE_END_TAG = "</page>";
  
  @Override
  public RecordReader<LongWritable,Text> getRecordReader(InputSplit inputSplit,
                                                         JobConf jobConf,
                                                         Reporter reporter) throws IOException {
    return new XmlRecordReader((FileSplit) inputSplit, jobConf);
  }
  
/**
* XMLRecordReader class to read through a given xml document to output xml
* blocks as records as specified by the start tag and end tag
*
*/
  public static class XmlRecordReader implements RecordReader<LongWritable,Text> {
    private final byte[] startTag;
    private final byte[] endTag;
    private final long start;
    private final long end;
    private final FSDataInputStream fsin;
    private final DataOutputBuffer buffer = new DataOutputBuffer();
    
    public XmlRecordReader(FileSplit split, JobConf jobConf) throws IOException {
      startTag = PAGE_START_TAG.getBytes();
      endTag = PAGE_END_TAG.getBytes();
      
      // open the file and seek to the start of the split
      start = split.getStart();
      end = start + split.getLength();
      Path file = split.getPath();
      FileSystem fs = file.getFileSystem(jobConf);
      fsin = fs.open(split.getPath());
      fsin.seek(start);
    }
    
    public boolean next(LongWritable key, Text value) throws IOException {
      if (fsin.getPos() < end) {
        if (findNextTag(startTag, false)) {
          try {
            buffer.write(startTag);
            if (findNextTag(endTag, true)) {
              key.set(fsin.getPos());
              value.set(buffer.getData(), 0, buffer.getLength());
              return true;
            }
          } finally {
            buffer.reset();
          }
        }
      }
      return false;
    }
    
    public LongWritable createKey() {
      return new LongWritable();
    }
    
    public Text createValue() {
      return new Text();
    }
    
    public long getPos() throws IOException {
      return fsin.getPos();
    }
    
    public void close() throws IOException {
      fsin.close();
    }
    
    public float getProgress() throws IOException {
      return (fsin.getPos() - start) / (float) (end - start);
    }
    
    private boolean findNextTag(byte[] tag, boolean saveToBuf) throws IOException {
      int i = 0;
      while (true) {
        int nextByte = fsin.read();
        if (nextByte == -1) return false;
        if (saveToBuf) buffer.write(nextByte);
        
        if (nextByte == tag[i]) {
          i++;
          if (i >= tag.length) return true;
        } else i = 0;

        if (!saveToBuf && i == 0 && fsin.getPos() >= end) return false;
      }
    }
  }
}