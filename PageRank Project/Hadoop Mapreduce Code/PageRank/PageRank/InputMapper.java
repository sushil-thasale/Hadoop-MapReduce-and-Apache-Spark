package PageRank;

import java.io.IOException;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

public class InputMapper extends Mapper<Object, Text, Text, PageData> {

	private static Pattern namePattern;
	private static Pattern linkPattern;
	private SAXParser saxParser;
	private SAXParserFactory spf;
	private XMLReader xmlReader;
	private List<String> linkPageNames;
	private static long pageCounter;

	static {

		// Keep only html pages not containing tilde (~).
		namePattern = Pattern.compile("^([^~]+)$");
		// Keep only html filenames ending relative paths and not containing
		// tilde (~).
		linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
	}

	public void setup(Context ctx) {

		pageCounter = 0;

		try {
			// Configure parser.
			spf = SAXParserFactory.newInstance();
			spf.setFeature(
					"http://apache.org/xml/features/nonvalidating/load-external-dtd",
					false);
			saxParser = spf.newSAXParser();
			xmlReader = saxParser.getXMLReader();
			linkPageNames = new LinkedList<>();
			xmlReader.setContentHandler(new WikiParser(linkPageNames));

		} catch (SAXNotRecognizedException | SAXNotSupportedException
				| ParserConfigurationException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void map(Object _k, Text line, Context ctx)
			throws InterruptedException, IOException {

		String readLine = line.toString();
		// Parser fills this list with linked page names.
		int delimLoc = readLine.indexOf(':');
		String pageName = readLine.substring(0, delimLoc);
		String html = readLine.substring(delimLoc + 1);
		Matcher matcher = namePattern.matcher(pageName);

		// skip html files, whose name contains (~)
		if (matcher.find()) {
			// Parse page and fill list of linked pages.
			linkPageNames.clear();
			try {
				xmlReader.parse(new InputSource(new StringReader(html)));
			} catch (Exception e) {
				// Discard ill-formatted pages.
			}

			PageData pd = new PageData();
			StringBuffer outlinks = new StringBuffer();

			for (String outlink : linkPageNames) {
				outlinks.append(outlink + "\t");
			}

			pd.outlinks = outlinks.toString();

			pageCounter += 1;
			ctx.write(new Text(pageName), pd.clone());
		}
	}

	/** Parses a Wikipage, finding links inside bodyContent div element. */
	private static class WikiParser extends DefaultHandler {
		/** List of linked pages; filled by parser. */
		private List<String> linkPageNames;
		/** Nesting depth inside bodyContent div element. */
		private int count = 0;

		public WikiParser(List<String> linkPageNames) {
			super();
			this.linkPageNames = linkPageNames;
		}

		@Override
		public void startElement(String uri, String localName, String qName,
				Attributes attributes) throws SAXException {
			super.startElement(uri, localName, qName, attributes);
			if ("div".equalsIgnoreCase(qName)
					&& "bodyContent"
							.equalsIgnoreCase(attributes.getValue("id"))
					&& count == 0) {
				// Beginning of bodyContent div element.
				count = 1;
			} else if (count > 0 && "a".equalsIgnoreCase(qName)) {
				// Anchor tag inside bodyContent div element.
				count++;
				String link = attributes.getValue("href");
				if (link == null) {
					return;
				}
				try {
					// Decode escaped characters in URL.
					link = URLDecoder.decode(link, "UTF-8");
				} catch (Exception e) {
					// Wiki-weirdness; use link as is.
				}
				// Keep only html filenames ending relative paths and not
				// containing tilde (~).
				Matcher matcher = linkPattern.matcher(link);
				if (matcher.find()) {
					linkPageNames.add(matcher.group(1));
				}
			} else if (count > 0) {
				// Other element inside bodyContent div.
				count++;
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName)
				throws SAXException {
			super.endElement(uri, localName, qName);
			if (count > 0) {
				// End of element inside bodyContent div.
				count--;
			}
		}

	}

	public void cleanup(Context ctx) {
		ctx.getCounter("totalPages", "").increment(pageCounter);
	}
}
