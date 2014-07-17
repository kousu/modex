/* graph.js
 *   draws a multi-timeseries linegraph from data passed over a websocket
 */

scope = {} //hacks
scope.data = [] //makes a data array that is safely accessible from anywhere in this file; sidesteps any weird js scoping rules that might kick in if we tried to make data a (pseudo)global

  
$(function() {
  // TODO move scope to the window.Game namespace 
  
  
  var parseDate = d3.time.format("%Y%m%d").parse;
    
  var margin = {top: 20, right: 80, bottom: 30, left: 50};
  
  //width = 960 - margin.left - margin.right,
  //height = 500 - margin.top - margin.bottom;
  width = $("#graph").width() - margin.left - margin.right;
  height = $("#graph").height() - margin.top - margin.bottom;
        
  var svg = d3.select("#graph").append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")"
        );
  var chart;
  
  nv.addGraph(function() {
    chart = nv.models.lineChart()
    .options({ width: width,    //it is super awkward that we have to define these; isn't d3 supposed to just figure this out?
               height: height,  //it might even be doing that, but it seems (try clicking the nvd3 legends on and off and watch
                                //the inspector---it resets the <rect> width exactly when you click) nvd3 has other plans in mind
               margin: {top: 10, right: 10, bottom: 15, left: 10}, //all these pixel measurements are hurtful
                        
               showXAxis: true,
               showYAxis: true,
               showLegend: true,
    });
    
    chart.xAxis
      .axisLabel("Time")
      .tickFormat(d3.format(",1f"));

    chart.yAxis
      .axisLabel("Quantity")
      .tickFormat(d3.format(',.1f'));
      
    chart.x(function(d,i) { return d.x});   // use the .x value for x-axis location (rather than i)
    nv.utils.windowResize(chart.update); //what does this do? do we need this?

    return chart;
  });

  // branch:databased TEMPORARY KLUDGE
  //  - redownload and redraw the whole dataset the whole time
  function redraw_graph() {
    //console.log("redraw_graph");
    d3.csv("/tables/activities", function(tbl) {
      // nvd3 wants its data like [ { key: "seriesname", values: [{x: ..., y: ...}, ...], color: "#FF00FF" }, .... ]
      // d3.csv gives its data like [ {"columnanme": value, ... } ... ]
      //  and  because we're using "Version 1" table structure, the series names are embeded in some (but not all) of the row names
      // so really we have
      // [ { "runID": -1, "time": 0, "series1": value, "series2": value}, 
      //   { "runID": -1, "time": 1, "series1": value, "series2": value}, .... ]
      // we need to map d3.csv -> nvd3 (..it is really annoying that nvd3 doesn't support csv out of the box)

      if(tbl.length > 0) {
        var timemin = +Infinity
        var timemax = -Infinity
        
        // option a: one pass with .push()es
        // option b: assume d3 gives consistent rows, and first extract the columnanmes from the 0th row, then do multiple passes with .map()
        serieses = Object.keys(tbl[0]).filter(function(e) { return !(e == "runID" || e == "time") })
        function series(name) { r= tbl.map(function(row) { 
            if(+row.time < timemin) { timemin = +row.time }
            if(+row.time > timemax) { timemax = +row.time }
            
            /*NB: +value is javascript for 'cast to numeric'*/
            rr =  {x: +row.time, y: +row[name]};  //split out for debugging weirdness
            //someting (nvd3??) is adding a 'series' value to rr. and somehow its managing to do it even though I'm recreating the dataset from scratch every time.
         
            // console.log(rr);
            return(rr);
            })
        
        //console.log(name, r);
        return(r);
        
        } 
        
        // overwrite the previous dataset
        scope.data = []
        serieses.forEach(function(s) {
          scope.data.push({key: s, values: series(s), color: getRandomColor()})
          })
        // aahhh i screwed up the indentation
        
        //finally, redraw the graph
        svg.datum(scope.data).call(chart);
        scope.chart = chart;

        //and update the time_slider
        time_slider = $("#time_slider")[0]
        time_slider.min = timemin
        // only update the 'max' value when we get too close to it, so that the slider actually moves by itself
        // note: timemax is also 'timecurrent'
        // XXX initialization is a problem; workaround it with "Infinity"
        if(time_slider.max == "Infinity" || +time_slider.max/2 < timemax) {
          time_slider.max = 3*timemax
        }
        time_slider.value = timemax
        // XXX copied from index.html
        var time_output = $("#time_output")[0];
        time_output.value = Math.floor(time_slider.value);
        }
      setTimeout(redraw_graph, 200); //only do the next redraw after the current redraw completes
      })
  }
  redraw_graph()
  
//Game.dataSocket.onmessage
  var notused = function(d) {
    //console.log("received data from (plain) websocket:", d)
    d = JSON.parse(d.data);
    var time = d[0]
    var series = d[1]
    //d.date = parseDate(d.date);
    
    //loop through and format data into line graph data for nv
    for(key in series) {
      addData(key, time, [series[key]]);
    }      

    // update the chart with the data
    svg.datum(scope.data)
       .call(chart);
    //scope.data.push(d)
    //draw()
  };
    
  //add Data to pre-existing line
  function addData(keyval,xval,yval) {
    var notthere = true
    //if there is already a line for the data add it to that line
    for(var i = 0; i < scope.data.length; i++) { // FIXME: switch to $.each() or $.map(). Looping over an array this way has caused me headaches in the past. 
      if(scope.data[i].key == keyval) {
        notthere = false;
        scope.data[i].values.push({x:xval, y:yval});
      }
    }
    
    //otherwise add a new line
    if(notthere){
      var new_line = {values: [{x:xval, y:yval}],
                      key: keyval,
                      color: getRandomColor()
                  }
      scope.data.push(new_line);
    }
  };
  
  // Just to be pretty
  function getRandomColor() {
    // some colour pairs appropriate for the colour blind, as shown by http://colorschemedesigner.com/:
    // rgb(42,23,177) vs rgb(255,201,0)
    //   (blueish)         (yellowish)
    var letters = '0123456789ABCDEF'.split('');
    var color = '#';
    for (var i = 0; i < 6; i++ ) {
      color += letters[Math.round(Math.random() * 15)];
    }
    return color;
  };
   
})
