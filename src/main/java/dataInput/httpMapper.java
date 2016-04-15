package dataInput;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class httpMapper extends Mapper<LongWritable, Text, NullWritable, MapWritable>{
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String emailline = value.toString();
		String[] fields = emailline.split("\t");
		if (fields.length != 27) {
//			throw new Exception("error line to parse http line, with fields length: "+fields.length);
			return;
		}
		String record_num = fields[0];
		String customer_id = fields[1];
		String recordtimeStr = fields[2];
		long recordtime = Long.parseLong(recordtimeStr);
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd"); 
		String dateyyyyMMdd = dateFormat.format(new Date(recordtime*1000));
		String site_url = fields[6];
		String http_title = fields[8];
		String ref = fields[15];

		MapWritable doc = new MapWritable();
		doc.put(new Text("id"), new Text(customer_id+"_http_"+record_num));
		doc.put(new Text("customer_id"), new Text(customer_id));
		doc.put(new Text("record_date"), new LongWritable(recordtime));
		doc.put(new Text("dateyyyyMMdd"), new Text(dateyyyyMMdd));
		doc.put(new Text("site_url"), new Text(site_url));
		doc.put(new Text("http_title"), new Text(http_title));
		doc.put(new Text("ref"), new Text(ref));
		
		context.write(NullWritable.get(), doc);
	}
}
