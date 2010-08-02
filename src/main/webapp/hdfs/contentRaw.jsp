<%@
page import="com.ning.metrics.action.hdfs.data.RowFileContentsIterator"
%><%@
page import="com.ning.metrics.action.hdfs.reader.HdfsEntry"
%><%@ page import="org.apache.commons.lang.StringUtils"%><%@
page contentType="text/plain"
%><jsp:useBean id="it" type="com.ning.metrics.action.hdfs.reader.HdfsListing" scope="request"/><%
                int startLine = 1;
                int endLine = -1;

                String range = request.getParameter("range");
                if (range != null) {
                    String[] rangeArray = StringUtils.split(range, "-");
                    if (rangeArray.length == 2) {
                        startLine = Integer.parseInt(rangeArray[0]);
                        endLine = Integer.parseInt(rangeArray[1]);
                    }
                }

                if (startLine > endLine) {
                    startLine = 1;
                    endLine = -1;
}
int snippetLength = endLine - startLine + 1;

int currentLine = 1;

for (int i = 0; i < it.getEntries().size(); i++) {
    HdfsEntry e = it.getEntries().get(i);
    RowFileContentsIterator content = e.getContent();
    while (content.hasNext()) { if (currentLine >= startLine && (currentLine <= endLine || endLine == -1)) {%><%= content.next().toString() %>
<%  } else { content.next(); } currentLine++; if (endLine != -1 && endLine < currentLine) { break; } } } %>