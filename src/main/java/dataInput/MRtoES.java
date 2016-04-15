package dataInput;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

public class MRtoES {
	private static Map<String,Class> typeMap = new HashMap<String,Class>();
	static {
		typeMap.put("http", httpMapper.class);
		typeMap.put("email",emailMapper.class);
	}
	private static boolean isCorrectType(String type){
		return typeMap.containsKey(type);
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String inputFile = null;
		String dataType = null;
		if (args.length == 2) {
			inputFile = args[0];
			dataType = args[1];
			if (!isCorrectType(dataType)) {
				System.out.println("input correct type");
				return;
			}
		} else {
			System.out.println("usage: hadoop -jar HbaseMapRedImport.jar <inputFile> <dataType>");
			return;
		}
		long start = System.currentTimeMillis();
		
		Configuration conf = new Configuration();
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);    
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false); 
		conf.set("es.nodes", "192.168.10.12:9200");
		conf.set("es.resource", "data_{dateyyyyMMdd}/"+dataType);   
		
		
		conf.set("es.mapping.id", "id");
		Job job = new Job(conf);
		job.setJobName("Test es-hadoop");
		job.setJarByClass(MRtoES.class);
		job.setMapperClass(typeMap.get(dataType));
		job.setOutputFormatClass(EsOutputFormat.class);
		job.setMapOutputValueClass(MapWritable.class);                       
		job.setNumReduceTasks(0);
		
		FileInputFormat.setInputPaths(job, inputFile);
		
		job.waitForCompletion(true);
		System.out.println("time elapse: "+(System.currentTimeMillis() - start)/1000);
	}

}
