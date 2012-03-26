package edu.uci.inforet;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CalculatePageRankReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    private static final float damping = 0.85F;
    
    public void reduce(Text page, Iterator<Text> values, OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
        boolean isExistingWikiPage = false;
        String[] split;
        float sumOfOtherPageRanks = 0;
        String links = "";
        String pageWithRank;
        
        // For each otherPage:
        // - check control characters
        // - calculate pageRank share <rank> / count(<links>)
        // - add the share to sumShareOtherPageRanks
        while(values.hasNext()){
            pageWithRank = values.next().toString();
            
            if(pageWithRank.equals("!")) {
                isExistingWikiPage = true;
                continue;
            }
            
            if(pageWithRank.startsWith("|")){
                links = "\t"+pageWithRank.substring(1);
                continue;
            }

            split = pageWithRank.split("\\t");
            
            float pageRank = Float.valueOf(split[1]);
            int countOutLinks = Integer.valueOf(split[2]);
            
            sumOfOtherPageRanks += (pageRank/countOutLinks);
        }

        if(!isExistingWikiPage) return;
        float newRank = damping * sumOfOtherPageRanks + (1-damping);

        out.collect(page, new Text(newRank + links));
    }
}
