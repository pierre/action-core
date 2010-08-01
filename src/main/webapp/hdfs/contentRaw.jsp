<%@
 page import="com.ning.metrics.action.hdfs.data.RowFileContentsIterator"
%><%@
page import="com.ning.metrics.action.hdfs.reader.HdfsEntry"
%><%@
page contentType="text/plain"
%><jsp:useBean
   id="it" type="com.ning.metrics.action.hdfs.reader.HdfsListing"
   scope="request"/><%
for (int i = 0; i < it.getEntries().size(); i++) {
    HdfsEntry e = it.getEntries().get(i);
    RowFileContentsIterator content = e.getContent();
    while (content.hasNext()) {%><%= content.next().toString() %>
<% } } %>