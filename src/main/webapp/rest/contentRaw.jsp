<%@
page import="com.ning.metrics.action.hdfs.data.Row"
%><%@
page import="com.ning.metrics.action.hdfs.reader.HdfsEntry"
%><%@
page import="org.apache.commons.lang.StringUtils"
%><%@
page import="java.util.Iterator"
%><%@
page contentType="text/plain"
%><%--
  ~ Copyright 2010-2011 Ning, Inc.
  ~
  ~ Ning licenses this file to you under the Apache License, version 2.0
  ~ (the "License"); you may not use this file except in compliance with the
  ~ License.  You may obtain a copy of the License at:
  ~
  ~    http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
  ~ WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
  ~ License for the specific language governing permissions and limitations
  ~ under the License.
  --%>
<jsp:useBean id="it" type="com.ning.metrics.action.hdfs.reader.HdfsListing" scope="request"/><%
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
    if (e.isDirectory()) {
        continue;
    }
    Iterator<Row> content = e.getContent();
    while (content.hasNext()) { if (currentLine >= startLine && (currentLine <= endLine || endLine == -1)) {%><%= content.next().toString() %>
<%  } else { content.next(); } currentLine++; if (endLine != -1 && endLine < currentLine) { break; } } } %>
