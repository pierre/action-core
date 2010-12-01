<%@ page import="com.ning.metrics.action.hdfs.data.Row" %>
<%@ page import="com.ning.metrics.action.hdfs.data.RowFileContentsIterator" %>
<%@ page import="com.ning.metrics.action.hdfs.reader.HdfsEntry" %>
<%@ page import="org.apache.commons.codec.binary.Base64" %>
<%@ page import="org.apache.commons.lang.StringUtils" %>
<%@ page import="java.net.URLEncoder" %>
<%@page contentType="text/html" %>

<%--
  ~ Copyright 2010 Ning, Inc.
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

<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"
"http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
    <meta http-equiv="Content-type" content="text/html; charset=utf-8">
    <title>HDFS on JSP</title>
    <script type="text/javascript" src="/static/js/jquery-1.3.2.min.js"></script>
    <link rel="stylesheet" href="/static/css/global.css" type="text/css">
</head>
<body>

<div id="header">
    <div class="wrapper">
        <h1>HDFS browser</h1>
    </div>
</div>

<div id="main">
    <div id="resultsWrapper">
        <table>
            <jsp:useBean id="it"
                         type="com.ning.metrics.action.hdfs.reader.HdfsListing"
                         scope="request">
            </jsp:useBean>
            <%
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
            %>
            <tr>
                <th>File <%= it.getPath() %> <% if (endLine != -1) { %>(lines <%= startLine %> to <%= endLine %>, <a
                        href="?path=<%= it.getPath() %>&amp;range=<%= endLine+1 %>-<%= endLine + snippetLength %>">next <%= snippetLength %>
                    lines</a>)<% }%>
                </th>
            </tr>
            <%
                int currentLine = 1;
                for (int i = 0; i < it.getEntries().size(); i++) {
                    HdfsEntry e = it.getEntries().get(i);
                    RowFileContentsIterator content = e.getContent();
                    while (content.hasNext()) {
                        if (currentLine >= startLine && (currentLine <= endLine || endLine == -1)) {
            %>
            <tr>
                <td>
                    <% Row currentContent = content.next(); %>
                    <a id="row_<%= currentLine %>" href="/rest/1.0/viewer?object=<%= URLEncoder.encode(new String(Base64.encodeBase64(currentContent.toJSON().getBytes())), "UTF-8") %>" target="_blank">
                        <%= currentContent.toString() %>
                    </a>
                </td>
            </tr>
            <%
                        }
                        else {
                            content.next();
                        }
                        currentLine++;

                        if (endLine != -1 && endLine < currentLine) {
                            break;
                        }
                    }
                }
            %>
        </table>
        <div style="clear:both;"></div>
    </div>
</div>
</body>
</html>