<%@ page import="com.ning.metrics.action.endpoint.HdfsEntry" %>

<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"
"http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
    <meta http-equiv="Content-type" content="text/html; charset=utf-8">
    <title>HDFS on JSP</title>
    <script type="text/javascript" src="/metrics.action/js/jquery-1.3.2.min.js"></script>
    <link rel="stylesheet" href="/metrics.action/css/global.css" type="text/css">
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
                             type="com.ning.metrics.action.endpoint.HdfsListing"
                             scope="request">
                </jsp:useBean>

                <tr>
                    <th>Name</th>
                    <th>Modification Time</th>
                    <th>Size</th>
                    <th>Replication size</th>
                </tr>
                <% if (it.getParentPath() != null) { %>
                <tr>
                    <td><a href="?dir=<%= it.getParentPath() %>&amp;type=json">..</a></td>
                    <td colspan="3"></td>
                </tr>
                <% } %>
                <tr>
                    <td><a href="?dir=<%= it.getPath() %>&amp;type=json">JSON listing</a></td>
                    <td colspan="3"></td>
                </tr>
                <tr>
                    <td><a href="?dir=<%= it.getPath() %>&amp;type=content&amp;recursive=false">. (dir
                        content)</a></td>
                    <td colspan="3"></td>
                </tr>
                <tr>
                    <td><a href="?dir=<%= it.getPath() %>&amp;type=content&amp;recursive=true">. (dir + subdir
                        content)</a></td>
                    <td colspan="3"></td>
                </tr>

                <%
                    for (int i = 0; i < it.getEntries().size(); i++) {
                        HdfsEntry e = it.getEntries().get(i);
                %>
                <tr>
                    <td>
                        <a href="?dir=<%= e.getPath() %>&amp;type=content"><%= e.getPath() %>
                        </a>
                        <a href="?dir=<%= e.getPath() %>&amp;type=content&amp;raw=true">(raw)</a>
                    </td>
                    <td><%= e.getModificationDate() %>
                    </td>
                    <td class="size"><%= e.getSize() %>
                    </td>
                    <td class="size"><%= e.getReplicatedSize() %>
                    </td>
                </tr>
                <%
                    }
                %></table>
        
        <div style="clear:both;"></div>
    </div>
</div>
</body>
</html>