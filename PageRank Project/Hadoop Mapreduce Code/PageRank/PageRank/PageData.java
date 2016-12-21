package PageRank;
/*
 * PagaData is a custom object storing (pageRank of Page P, outlinks of P)
 * */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

class PageData implements WritableComparable<PageData> {
	public double pageRank;
	public String outlinks;

	public PageData() {
		pageRank = 0;
		outlinks = "";
	}

	public PageData clone() {
		PageData pd = new PageData();
		pd.pageRank = pageRank;
		pd.outlinks = outlinks;
		return pd;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(pageRank);
		Text textOutLinks = new Text(this.outlinks);
		textOutLinks.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		pageRank = in.readDouble();
		Text textOutLinks = new Text();
		textOutLinks.readFields(in);
		this.outlinks = textOutLinks.toString();
	}

	public String toString() {
		return (pageRank + "\t" + outlinks);
	}

	@Override
	public int compareTo(PageData arg0) {
		return this.toString().compareTo(arg0.toString());
	}

	@Override
	public int hashCode() {
		return this.toString().hashCode();
	}

	public boolean equals(PageData yy) {
		return this.toString().equals(yy.toString());
	}

	@Override
	public boolean equals(Object oo) {
		return this.equals((PageData) oo);
	}
}
