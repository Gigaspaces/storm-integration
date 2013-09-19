[![Build Status](https://secure.travis-ci.org/OpenSpaces/RESTData.png)](http://travis-ci.org/OpenSpaces/RESTData)


<h2>Overview</h2>

<p>&nbsp;The project provides REST support for the GigaSpace Space API using HTTP methods in the following manner:</p>
<ol>
  <li>GET - can be used to perform a readByID or a readMultiple action by a space query.</li>
	<li>POST - can be used to perform a write / writeMultiple action.<br/>
		<b>Notice:</b> POST is mapped to a WriteOnly action.<br/>
		An exception will be thrown when trying to write an object which already exists in space.</li>
	<li>PUT - can be used to perform a single or multiple write or update actions.</li>
	<li>DELETE - can be used to perform take / takeMultiple actions either by ID or by a space query.</li>
</ol>


<h2><a name="ProjectDocumentation-Examples"></a>Examples</h2>

<ul>
	<li>WriteMultiple
<br clear="all" />
<div class="preformatted panel" style="border-width: 1px;"><div class="preformattedContent panelContent">
<pre>curl -XPOST -d '[{"id":"1", "data":"testdata", "data2":"common", "nestedData" : {"nestedKey1":"nestedValue1"}}, {"id":"2", "data":"testdata2", "data2":"common", "nestedData" : {"nestedKey2":"nestedValue2"}}, {"id":"3", "data":"testdata3", "data2":"common", "nestedData" : {"nestedKey3":"nestedValue3"}}]' http://localhost:8080/WebApp/rest/data/Item
</pre>
</div></div></li>
	<li>readMultiple
	<ul>
		<li>&nbsp;<a href="http://localhost:8080/WebApp/rest/data/Item/_criteria?q=data2='common" rel="nofollow">http://localhost:8080/WebApp/rest/data/Item/_criteria?q=data2='common</a>'</li>
		<li>&nbsp;<a href="http://localhost:8080/WebApp/rest/data/Item/_criteria?q=id=%271%27%20or%20id=%272%27%20or%20id=%273%27" rel="nofollow">http://localhost:8080/WebApp/rest/data/Item/_criteria?q=id=%271%27%20or%20id=%272%27%20or%20id=%273%27</a> (the url is encoded, the query is "id='1' or id='2' or id='3'")</li>
	</ul>
	</li>
	<li><font color="">readById</font><br clear="all" />
	<ul>
		<li><a href="http://localhost:8080/WebApp/rest/data/Item/1" rel="nofollow">http://localhost:8080/WebApp/rest/data/Item/1</a></li>
		<li><a href="http://localhost:8080/WebApp/rest/data/Item/2" rel="nofollow">http://localhost:8080/WebApp/rest/data/Item/2</a></li>
		<li><a href="http://localhost:8080/WebApp/rest/data/Item/3" rel="nofollow">http://localhost:8080/WebApp/rest/data/Item/3</a></li>
	</ul>
	</li>
	<li><font color="#000000">updateMultiple</font><div class="preformatted panel" style="border-width: 1px;"><div class="preformattedContent panelContent">
<pre>curl -XPUT -d '[{"id":"1", "data":"testdata", "data2":"commonUpdated", "nestedData" : {"nestedKey1":"nestedValue1"}}, {"id":"2", "data":"testdata2", "data2":"commonUpdated", "nestedData" : {"nestedKey2":"nestedValue2"}}, {"id":"3", "data":"testdata3", "data2":"commonUpdated", "nestedData" : {"nestedKey3":"nestedValue3"}}]' http://localhost:8080/WebApp/rest/data/Item
</pre>
</div></div>-> see that data2 field is updated: <a href="http://localhost:8080/WebApp/rest/data/Item/_criteria?q=data2='commonUpdated'" rel="nofollow">http://localhost:8080/WebApp/rest/data/Item/_criteria?q=data2='commonUpdated'</a></li>
</ul>


<ul>
	<li>&nbsp;<font color="#000000">single nested update</font><div class="preformatted panel" style="border-width: 1px;"><div class="preformattedContent panelContent">
<pre>curl -XPUT -d '{"id":"1", "data":"testdata", "data2":"commonUpdated", "nestedData" : {"nestedKey1":"nestedValue1Updated"}}' http://localhost:8080/WebApp/rest/data/Item
</pre>
</div></div>-> <font color="#000000">see that Item1 nested field is updated:</font><font color="#000000">&nbsp;</font>http://localhost:8080/WebApp/rest/data/Item/1</li>
</ul>


<ul>
	<li><font color="#000000">takeMultiple (url is encoded, the query is "id=1 or id=2")</font><div class="preformatted panel" style="border-width: 1px;"><div class="preformattedContent panelContent">
<pre>curl -XDELETE http://localhost:8080/WebApp/rest/data/Item/_criteria?q=id=%271%27%20or%20id=%272%27
</pre>
</div></div>-> see that only Item3 remains: <a href="http://localhost:8080/WebApp/rest/data/Item/_criteria?q=id=%271%27%20or%20id=%272%27%20or%20id=%273%27" rel="nofollow">http://localhost:8080/WebApp/rest/data/Item/_criteria?q=id=%271%27%20or%20id=%272%27%20or%20id=%273%27</a></li>
</ul>


<ul>
	<li><font color="#000000">takeById</font><div class="preformatted panel" style="border-width: 1px;"><div class="preformattedContent panelContent">
<pre>curl -XDELETE "http://localhost:8080/WebApp/rest/data/Item/3"
</pre>
</div></div><br/>
-> see that Item3 does not exists: <a href="http://localhost:8080/WebApp/rest/data/Item/_criteria?q=id=%271%27%20or%20id=%272%27%20or%20id=%273%27" rel="nofollow">http://localhost:8080/WebApp/rest/data/Item/_criteria?q=id=%271%27%20or%20id=%272%27%20or%20id=%273%27</a></li>
</ul>


<h2><a name="ProjectDocumentation-SetupInstructions"></a>Setup Instructions</h2>

<p>1.download the project from SVN</p>

<p>2.edit "/RESTData/src/main/webapp/WEB-INF/config.properties"? to include your space url, for example: <tt>spaceUrl=jini://&#42;/&#42;/testSpace?groups=restdata</tt></p>

<p>3.package the project using maven: "mvn package"<br/>
this will run the unit tests and package the project to a war file located at /target/RESTData.war</p>

<p>4.deploy the war file. </p>
