$(function() {
/*
 *   TODO
 *  
 * [x] make the d3 plot update
 *  [x] subproblem: less doesn't work because it's nonlocal
 *  [x] subproblem: jquery reruns .ready() handlers if one of them crashes??
 *  [ ] use enter() and exit() instead of redrawing everything on every update
 *  [ ] don't jump the axes all the time
 *  [ ] construct a legend instead of labelling lines
 *  [ ] make the lines *independent*: we should be able to throw arbitary coordinate pairs into each list and have them arrange themselves properly; it shouldn't be forced
 *   
 */

   scope = {} //hacks
   scope.data = [] //makes a data array that is safely accessible from anywhere in this file; sidesteps any weird js scoping rules that might kick in if we tried to make data a (pseudo)global
    
    var margin = {top: 20, right: 80, bottom: 30, left: 50},
    
    //width = 960 - margin.left - margin.right,
    //height = 500 - margin.top - margin.bottom;
    width = $("#graph").width() - margin.left - margin.right
    height = $("#graph").height() - margin.top - margin.bottom;

    var parseDate = d3.time.format("%Y%m%d").parse;

    var x = d3.time.scale()
        .range([0, width]);

    var y = d3.scale.linear()
        .range([height, 0]);

    var color = d3.scale.category10();

    var xAxis = d3.svg.axis()
        .scale(x)
        .orient("bottom");

    var yAxis = d3.svg.axis()
        .scale(y)
        .orient("left");

    var line = d3.svg.line()
        .interpolate("basis")
        .x(function(d) { return x(d.date); })
        .y(function(d) { return y(d.temperature); });

    
    
    
    data_socket = new WebSocket("ws://" + location.host + "/ws") //our websocket sits at /ws (TODO(kousu): reorg this)
    data_socket.onopen = function() { 
       this.send(""); //poke the server to get data out
    }
    data_socket.onmessage = function(d) {
      d = JSON.parse(d.data);
      //console.log("received data from (plain) websocket:")
      d.date = parseDate(d.date)
      //console.log(d);
      scope.data.push(d)
      draw()
    }
    
    /*
    d3.tsv("assets/data/static_lightbulbs.tsv", function(error, data) {
    
      data.forEach(function(d) {
        d.date = parseDate(d.date);
      });
      
      scope.data = data;
      
      draw()
      })
      */
      
   // the trickiest part of this problem is that it is several line graphs, not just ones
   
   function draw() {
    data = scope.data; //hacks
      
    //console.log("plotting this data array:", data)
      
    //console.log("creating svg")
    $("svg").detach() //kill old svg, if there is one
    var svg = d3.select("#graph").append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")"
        );
      
      color.domain(d3.keys(data[0]).filter(function(key) { return key !== "date"; }));

      var cities = color.domain().map(function(name) {
        return {
          name: name,
          values: data.map(function(d) {
            return {date: d.date, temperature: +d[name]};
          })
        };
      });

      x.domain(d3.extent(data, function(d) { return d.date; }));

      y.domain([ //compute the outermost values we need on the axes to show all the data
        d3.min(cities, function(c) { return d3.min(c.values, function(v) { return v.temperature; }); }),
        d3.max(cities, function(c) { return d3.max(c.values, function(v) { return v.temperature; }); })
      ]);

      svg.append("g")
          .attr("class", "x axis")
          .attr("transform", "translate(0," + height + ")")
          .call(xAxis);

      svg.append("g")
          .attr("class", "y axis")
          .call(yAxis)
        .append("text")
          .attr("transform", "rotate(-90)")
          .attr("y", 6)
          .attr("dy", ".71em")
          .style("text-anchor", "end")
          .text("Temperature (ºF)"); //tempurature?

      //'city' is named because this code was stolen from a demo showing
      // off using d3 to plot multiple city temperature lines
      // 'city' is a <g> tag containing a <path>: the line, and
      //   a <text>: the label at the end of the line
      var city = svg.selectAll(".city")
          .data(cities)
        .enter().append("g")
          .attr("class", "city");

     // this is <path> tag
     // 
      city.append("path")
          .attr("class", "line")
          .attr("d", function(d) { return line(d.values); /*this call returns the actual line in the format desired by svg <path> elements */ })
          .style("stroke", function(d) { return color(d.name); });

      // this is the <text> tag which labels the end of the line
      city.append("text")
          .datum(function(d) { return {name: d.name, value: d.values[d.values.length - 1]}; })
          .attr("transform", function(d) { return "translate(" + x(d.value.date) + "," + y(d.value.temperature) + ")"; })
          .attr("x", 3)
          .attr("dy", ".35em")
          .text(function(d) { return d.name; });
   }
});
