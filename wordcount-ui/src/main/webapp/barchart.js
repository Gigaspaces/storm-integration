
function barChart(container){

var m = [30, 10, 10, 60],
    w = 512 - m[1] - m[3],
    h = 256 - m[0] - m[2];

var format = d3.format(",.0f");

var x = d3.scale.linear().range([0, w]),
    y = d3.scale.ordinal().rangeRoundBands([0, h], .1);

var xAxis = d3.svg.axis().scale(x).orient("top").tickSize(-h),
    yAxis = d3.svg.axis().scale(y).orient("left").tickSize(0);

var svg = d3.select("#"+container).append("svg")
    .attr("width", w + m[1] + m[3])
    .attr("height", h + m[0] + m[2])
  .append("g")
    .attr("transform", "translate(" + m[3] + "," + m[0] + ")");


  // Parse numbers, and sort by value.
  ww.forEach(function(d) { d.count = +d.count; });
  //ww.sort(function(a, b) { return b.size - a.size; });  //already sorted

  // Set the scale domain.
  x.domain([0, d3.max(ww, function(d) { return d.count; })]);
  y.domain(ww.map(function(d) { return d.text; }));

  var bar = svg.selectAll("g.bar")
      .data(ww)
    .enter().append("g")
      .attr("class", "bar")
      .attr("transform", function(d) { return "translate(0," + y(d.text) + ")"; });

  bar.append("rect")
      .attr("width", function(d) { return x(d.count); })
      .attr("height", y.rangeBand());

  bar.append("text")
      .attr("class", "value")
      .attr("x", function(d) { return x(d.count); })
      .attr("y", y.rangeBand() / 2)
      .attr("dx", -3)
      .attr("dy", ".35em")
      .attr("text-anchor", "end")
      .text(function(d) { return format(d.count); });

  svg.append("g")
      .attr("class", "x axis")
      .call(xAxis);

  svg.append("g")
      .attr("class", "y axis")
      .call(yAxis);
//});
      
  function update(){
	  var m = [30, 10, 10, 60],
	    w = 512 - m[1] - m[3],
	    h = 256 - m[0] - m[2];

	var format = d3.format(",.0f");

	var x = d3.scale.linear().range([0, w]),
	    y = d3.scale.ordinal().rangeRoundBands([0, h], .1);

	var xAxis = d3.svg.axis().scale(x).orient("top").tickSize(-h),
	    yAxis = d3.svg.axis().scale(y).orient("left").tickSize(0);
	    
	d3.select("#"+container).select("svg").remove();
	
	var svg = d3.select("#"+container).append("svg")
	    .attr("width", w + m[1] + m[3])
	    .attr("height", h + m[0] + m[2])
	  .append("g")
	    .attr("transform", "translate(" + m[3] + "," + m[0] + ")");

	  // Set the scale domain.
	  x.domain([0, d3.max(ww, function(d) { return d.count; })]);
	  y.domain(ww.map(function(d) { return d.text; }));
	  
	  var bar = svg.selectAll("g.bar")
	      .data(ww)
	    .enter().append("g")
	      .attr("class", "bar")
	      .attr("transform", function(d) { return "translate(0," + y(d.text) + ")"; });

	  bar.append("rect")
	      .attr("width", function(d) { return x(d.count); })
	      .attr("height", y.rangeBand());

	  bar.append("text")
	      .attr("class", "value")
	      .attr("x", function(d) { return x(d.count); })
	      .attr("y", y.rangeBand() / 2)
	      .attr("dx", -3)
	      .attr("dy", ".35em")
	      .attr("text-anchor", "end")
	      .text(function(d) { return format(d.count); });

	  svg.append("g")
	      .attr("class", "x axis")
	      .call(xAxis);

	  svg.append("g")
	      .attr("class", "y axis")
	      .call(yAxis);
  }
  setInterval(update,2000);
  
}



