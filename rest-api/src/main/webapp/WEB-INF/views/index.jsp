<%@page import="java.net.InetAddress"%>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1"%>
<% String ip = InetAddress.getLocalHost().getHostAddress(); 
   String context = request.getContextPath();
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<title>GigaSpaces REST API</title>
</head>
<body>
	<center><img alt="logo" src="<%=context%>/resources/logo.png"></center><br/>
	
	<h1>REST Data (Space API)</h1>
	<h2>Examples</h2>
	<h3>follow the steps one by one to experience the whole feature set</h3>
	<ul>
		<li> writeMultiple
		<ul>
			<li><p>curl -XPOST -d '[{"id":"1", "data":"testdata", "data2":"common", "nestedData" : {"nestedKey1":"nestedValue1"}}, {"id":"2", "data":"testdata2", "data2":"common", "nestedData" : {"nestedKey2":"nestedValue2"}}, {"id":"3", "data":"testdata3", "data2":"common", "nestedData" : {"nestedKey3":"nestedValue3"}}]' http://<%=ip%>:8080<%=context%>/rest/data/Item</p></li>
		</ul>
		</li>
		<li> readMultiple
		<ul> 
			<li><p><a href="http://<%=ip%>:8080<%=context%>/rest/data/Item/_criteria?q=data2='common'">http://<%=ip%>:8080<%=context%>/rest/data/Item/_criteria?q=data2='common'</a></p></li>
			<li><p><a href="http://<%=ip%>:8080<%=context%>/rest/data/Item/_criteria?q=id='1' or id='2' or id='3'">http://<%=ip%>:8080<%=context%>/rest/data/Item/_criteria?q=id='1' or id='2' or id='3'</a></p></li>
		</ul>
		</li>		
		<li> readById
		<ul>
			<li><p><a href="http://<%=ip%>:8080<%=context%>/rest/data/Item/1">http://<%=ip%>:8080<%=context%>/rest/data/Item/1</a></p></li>
			<li><p><a href="http://<%=ip%>:8080<%=context%>/rest/data/Item/2">http://<%=ip%>:8080<%=context%>/rest/data/Item/2</a></p></li>
			<li><p><a href="http://<%=ip%>:8080<%=context%>/rest/data/Item/3">http://<%=ip%>:8080<%=context%>/rest/data/Item/3</a></p></li>
		</ul>
		</li>
		<li> updateMultiple
		<ul> 
			<li><p>curl -XPUT -d '[{"id":"1", "data":"testdata", "data2":"commonUpdated", "nestedData" : {"nestedKey1":"nestedValue1"}}, {"id":"2", "data":"testdata2", "data2":"commonUpdated", "nestedData" : {"nestedKey2":"nestedValue2"}}, {"id":"3", "data":"testdata3", "data2":"commonUpdated", "nestedData" : {"nestedKey3":"nestedValue3"}}]' http://<%=ip%>:8080<%=context%>/rest/data/Item</p></li>
			<p>see that data2 field is updated: <a href="http://<%=ip%>:8080<%=context%>/rest/data/Item/_criteria?q=data2='commonUpdated'">http://<%=ip%>:8080<%=context%>/rest/data/Item/_criteria?q=data2='commonUpdated'</a></p>
		</ul>
		</li>
		<li> single nested update
		<ul> 
			<li><p>curl -XPUT -d '{"id":"1", "data":"testdata", "data2":"commonUpdated", "nestedData" : {"nestedKey1":"nestedValue1Updated"}}' http://<%=ip%>:8080<%=context%>/rest/data/Item</p></li>	
			<p>see that Item1 nested field is updated: <a href="http://<%=ip%>:8080<%=context%>/rest/data/Item/1">http://<%=ip%>:8080<%=context%>/rest/data/Item/1</a></p>								
		</ul>
		</li>
				
		<li> takeMultiple (the url is encoded, the query is "id=1 or id=2"):
		<ul>
			<li><p>curl -XDELETE http://<%=ip%>:8080<%=context%>/rest/data/Item/_criteria?q=id=%271%27%20or%20id=%272%27</p></li>
			<p>see that only Item3 remains: <a href="http://<%=ip%>:8080<%=context%>/rest/data/Item/_criteria?q=id='1' or id='2' or id='3'">http://<%=ip%>:8080<%=context%>/rest/data/Item/_criteria?q=id='1' or id='2' or id='3'</a></p>
		</ul>
		</li>
				
		<li> takeById
		<ul>
			<li><p>curl -XDELETE "http://<%=ip%>:8080<%=context%>/rest/data/Item/3"</p></li>
			<p>see that item3 does not exist,response status 404 and the json {"error":"object not found"} <a href="http://<%=ip%>:8080<%=context%>/rest/data/Item/_criteria?q=id='1' or id='2' or id='3'">http://<%=ip%>:8080<%=context%>/rest/data/Item/_criteria?q=id='1' or id='2' or id='3'</a></p>
		</ul>
				
	</ul>
</body>
</html>