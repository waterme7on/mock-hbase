package org.waterme7on.hbase.conf;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.waterme7on.hbase.Writable;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import com.google.common.base.Strings;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

public class Configuration implements Iterable<Map.Entry<String, String>>, Writable {
    private Properties properties;
    public Configuration(){
        this.properties = null;
    }
    public static void main(String[] args) throws Exception {
        (new Configuration()).writeXml((OutputStream)System.out);
    }
    public void set(String name, String value) {
        this.set(name, value, (String)null);
    }

    public void set(String name, String value, String source) {
        this.getProps().setProperty(name, value);
    }
    public void clear() {
        this.getProps().clear();
    }
    protected synchronized Properties getProps() {
        if (this.properties == null) {
            this.properties = new Properties();
            this.loadProps(this.properties, 0, true);
        }

        return this.properties;
    }
    private synchronized void loadProps(Properties props, int startIdx, boolean fullReload) {
        // TODO
    }
    public Iterator<Map.Entry<String, String>> iterator() {
        Properties props = this.getProps();
        Map<String, String> result = new HashMap();
        synchronized(props) {
            Iterator i$ = props.entrySet().iterator();

            while(i$.hasNext()) {
                Map.Entry<Object, Object> item = (Map.Entry)i$.next();
                if (item.getKey() instanceof String && item.getValue() instanceof String) {
                    result.put((String)item.getKey(), (String)item.getValue());
                }
            }

            return result.entrySet().iterator();
        }
    }
    public void readFields(DataInput in) throws IOException {
        this.clear();
        int size = WritableUtils.readVInt(in);

        for(int i = 0; i < size; ++i) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            this.set(key, value);
            String[] sources = WritableUtils.readCompressedStringArray(in);
//            if (sources != null) {
//                this.putIntoUpdatingResource(key, sources);
//            }
        }

    }
    public void write(DataOutput out) throws IOException {
        Properties props = this.getProps();
        WritableUtils.writeVInt(out, props.size());
        Iterator i$ = props.entrySet().iterator();

        while(i$.hasNext()) {
            Map.Entry<Object, Object> item = (Map.Entry)i$.next();
            Text.writeString(out, (String)item.getKey());
            Text.writeString(out, (String)item.getValue());
        }
    }

    public void writeXml(OutputStream out) throws IOException {
        this.writeXml((Writer)(new OutputStreamWriter(out, "UTF-8")));
    }

    public void writeXml(Writer out) throws IOException {
        this.writeXml((String)null, out);
    }

    public void writeXml(String propertyName, Writer out) throws IOException, IllegalArgumentException {
        Document doc = this.asXmlDocument(propertyName);

        try {
            DOMSource source = new DOMSource(doc);
            StreamResult result = new StreamResult(out);
            TransformerFactory transFactory = TransformerFactory.newInstance();
            Transformer transformer = transFactory.newTransformer();
            transformer.transform(source, result);
        } catch (TransformerException var8) {
            throw new IOException(var8);
        }
    }
    private synchronized Document asXmlDocument(String propertyName) throws IOException, IllegalArgumentException {
        Document doc;
        try {
            doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
        } catch (ParserConfigurationException var5) {
            throw new IOException(var5);
        }

        Element conf = doc.createElement("configuration");
        doc.appendChild(conf);
        conf.appendChild(doc.createTextNode("\n"));
        if (!Strings.isNullOrEmpty(propertyName)) {
            if (!this.properties.containsKey(propertyName)) {
                throw new IllegalArgumentException("Property " + propertyName + " not found");
            }

            this.appendXMLProperty(doc, conf, propertyName);
            conf.appendChild(doc.createTextNode("\n"));
        } else {
            Enumeration<Object> e = this.properties.keys();

            while(e.hasMoreElements()) {
                this.appendXMLProperty(doc, conf, (String)e.nextElement());
                conf.appendChild(doc.createTextNode("\n"));
            }
        }

        return doc;
    }

    private synchronized void appendXMLProperty(Document doc, Element conf, String propertyName) {
        if (!Strings.isNullOrEmpty(propertyName)) {
            String value = this.properties.getProperty(propertyName);
            if (value != null) {
                Element propNode = doc.createElement("property");
                conf.appendChild(propNode);
                Element nameNode = doc.createElement("name");
                nameNode.appendChild(doc.createTextNode(propertyName));
                propNode.appendChild(nameNode);
                Element valueNode = doc.createElement("value");
                valueNode.appendChild(doc.createTextNode(this.properties.getProperty(propertyName)));
                propNode.appendChild(valueNode);
//                Element finalNode = doc.createElement("final");
//                finalNode.appendChild(doc.createTextNode(String.valueOf(this.finalParameters.contains(propertyName))));
//                propNode.appendChild(finalNode);
            }
        }

    }}
