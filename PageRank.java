package edu.uci.inforet;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class PageRank {
	
	private static NumberFormat nf = new DecimalFormat("00");

	public static void main(String[] args) throws Exception {
        PageRank pageRanking = new PageRank();
        // Job 1: Parse page
        pageRanking.parseXmlPages("wiki/in", "wiki/out/iter00");
        
        int runs = 0;
        for (; runs < 3; runs++) {
            //Job 2: Calculate new rank
            pageRanking.calculatePageRank("wiki/out/iter"+nf.format(runs), "wiki/out/iter"+nf.format(runs + 1));
        }
 
        //Job 3: Order by rank
        pageRanking.sortByPage("wiki/out/iter"+nf.format(runs), "wiki/result");
        
 
    }
 
    public void parseXmlPages(String inputPath, String outputPath) throws IOException {
        
    	JobConf job = new JobConf(new Configuration(), PageRank.class);
        job.setJobName("Parse-XML-Links");
        
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        // Mahout class to Parse XML + config
        job.setInputFormat(XmlInputFormat.class);
        
        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setMapperClass(ParseXmlMapper.class);
        job.setReducerClass(ParseXmlReducer.class);
 
        JobClient.runJob(job);
    }
    
    private void calculatePageRank(String inputPath, String outputPath) throws IOException {
        
    	JobConf job = new JobConf(PageRank.class);
        job.setJobName("Calculate-PageRank");
        
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        job.setMapperClass(CalculatePageRankMapper.class);
        job.setReducerClass(CalculatePageRankReducer.class);
 
        JobClient.runJob(job);
    }
 
    private void sortByPage(String inputPath, String outputPath) throws IOException {
        JobConf conf = new JobConf(PageRank.class);
 
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(FloatWritable.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
 
 
        conf.setMapperClass(SortResultByPage.class);
 
        JobClient.runJob(conf);
    }
}
