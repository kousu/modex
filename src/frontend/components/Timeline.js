Crafty.c('AddScenario', {
  scenarioCount: 1,
  init: function() {
    // make sure that we have all the entities required
    this.requires('2D, Canvas, Mouse, plusButton');

    // position the button at the bottom of the page
    var positionY = Crafty.viewport.height;
    this.attr({x: 20, y: positionY-50-20, w: 100, h: 100})
        // to create a new scenario/timeline, press this button
        .bind('Click', function(e){
          Crafty.e('Timeline,');
        });
    //Crafty.e('Timeline');
  },

  timeline: function(length) {
    this.length = length;
    console.log(this.length);
  },
});


Crafty.c('Timeline', {
  init: function() {
    this.requires('2D, Canvas, Color, Mouse, timelineBackground');
    this.bind('Click', function(e){
      console.log(e);
    })
    .attr({x: 100, y: 100, w: 700, h: 25});
//    .color('green');
  },

  timeline: function(length) {
    this.length = length;
    console.log(this.length);
  },
  //length: 10,
});