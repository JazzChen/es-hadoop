package dataInput;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class emailMapper extends Mapper<LongWritable, Text, NullWritable, MapWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String emailline = value.toString();
		String[] fields = emailline.split("\t");
		if (fields.length != 30) {
//			throw new Exception("error line to parse Email line, with fields length: "+fields.length);
			return;
		}
		String record_num = fields[0];
	  	String customer_id = fields[1];
		long recordtime = Long.parseLong(fields[5]);
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd"); 
		String dateyyyyMMdd = dateFormat.format(new Date(recordtime*1000));
	  	String sender = fields[8];
		int recipient_num = Integer.parseInt(fields[9]);
		String[] recipient = new String[3];
		recipient[0] = fields[10].trim();
		recipient[1] = fields[11].trim();
		recipient[2] = fields[12].trim();
		
		int copyto_num = Integer.parseInt(fields[13]);
		String[] copyto = new String[3];
		copyto[0] = fields[14].trim();
		copyto[1] = fields[15].trim();
		copyto[2] = fields[16].trim();
		
		String email_title = fields[17];
		
		int attach_num = Integer.parseInt(fields[18]);
		String[] attach = new String[3];
		attach[0] = fields[19].trim();
		attach[1] = fields[20].trim();
		attach[2] = fields[21].trim();
		
		List<String> recipients = new ArrayList<String>();
		List<String> copytos = new ArrayList<String>();
		List<String> attaches = new ArrayList<String>();
		//get recipient
		for (int i = 0; i < recipient.length; i++) {
			if (recipient[i].compareTo("null-rec") != 0) {
				String splitword = recipient[i].substring(recipient[i].length()-1);
				String[] rec = recipient[i].split(splitword);
				for (int j = 0; j < rec.length; j++) {
					recipients.add(rec[j]);
				}
			}
		}
		if (recipients.size() != recipient_num) {
			System.out.println("actual recipients count is not match record in data!");
		}
		//get copyto
		for (int i = 0; i < copyto.length; i++) {
			if (copyto[i].compareTo("null-cc") != 0) {
				String splitword = copyto[i].substring(copyto[i].length()-1);
				String[] cc = copyto[i].split(splitword);
				for (int j = 0; j < cc.length; j++) {
					copytos.add(cc[j]);
				}
			}
		}
		if (copytos.size() != copyto_num) {
			System.out.println("actual copyto count is not match record in data!");

		}
		//get attach
		for (int i = 0; i < attach.length; i++) {
			if (attach[i].compareTo("null-attach") != 0) {
				String splitword = attach[i].substring(attach[i].length()-1);
				String[] at = attach[i].split(splitword);
				for (int j = 0; j < at.length; j++) {
					attaches.add(at[j]);
				}
			}
		}
		if (attaches.size() != attach_num) {
			System.out.println("actual attaches count is not match record in data!");
		}
		
		MapWritable doc = new MapWritable();
		doc.put(new Text("id"), new Text(customer_id+"_email_"+record_num));
		doc.put(new Text("customer_id"), new Text(customer_id));
		doc.put(new Text("record_date"), new LongWritable(recordtime));
		doc.put(new Text("dateyyyyMMdd"), new Text(dateyyyyMMdd));
//		doc.put(new Text("record_date"), new LongWritable(recordtime));
		doc.put(new Text("sender"), new Text(sender));
		doc.put(new Text("email_title"), new Text(email_title));
		doc.put(new Text("recipient"), new ArrayWritable(recipients.toArray(new String[recipients.size()])));
		doc.put(new Text("copyto"), new ArrayWritable(copytos.toArray(new String[copytos.size()])));
		doc.put(new Text("attach"), new ArrayWritable(attaches.toArray(new String[attaches.size()])));
		
		context.write(NullWritable.get(), doc);
	}
}
