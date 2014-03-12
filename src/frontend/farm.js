
"use strict";
var farm = function($) {

    return {



        config : {
            width : 1280,
            height : 720
       
        },

        Questions : null,

        /**
         * Initializes Quiz application
         */
        initialize : function() {
            

            // init Crafty
            Crafty.init(this.config.width, this.config.height);
            //Crafty.canvas.init();

            

            // set bacgroundd
            Crafty.background("url('images/background.png')");

            
            // Run intro
            Crafty.scene("intro");
            

            // load main sprite sheet
            Crafty.sprite(5, "images/menu_sprite.png", {
                
                   
                //Titles
                logoText : [0, 90, 106, 25],
                setupTitle : [156, 0, 100, 20],
                roleTitle : [0, 116, 27, 26],
                goalTitle : [0, 176, 70, 20],

                // Reusable Elements
                menuBtn : [0, 0, 50, 20],
                squareBtn : [106, 0, 20, 20],
                backArrow : [126, 0, 20, 20],
                exitBtn : [50, 116, 20, 20],
                blueCircle : [207, 90, 16, 20],
                greenCircle : [223, 90, 16, 20],
                modalWindow : [106, 110, 150, 90],
                startText : [156, 46, 50, 20],

                //Menu text
                playText: [0, 24, 50, 20],
                setupText: [0, 46, 50, 20],
                loadText : [0, 68, 50, 20],
                scenariosText : [106, 24, 50, 20],

            


                //Setup Screen Stuff
                govtIcon : [60, 136, 20, 20],
                condIcon : [40, 136, 20, 20],
                natIcon : [20, 136, 20, 20],
                timeIcon : [0, 136, 20, 20],
                loadscText : [206, 46, 50, 20],
                saveText : [106, 46, 50, 20],
                govtText : [106, 68, 50, 6],
                condText : [156, 68, 50, 6],
                natText : [206, 24, 50, 6],
                timeText : [206, 68, 50, 6],
                breakLine : [90, 44, 88, 1],


                //Set End Goals Screen
                happinessIcon : [40, 156, 20, 20],
                cornIcon : [20, 156, 20, 20],
                gdpIcon : [0, 156, 20, 20],
                loadgText : [106, 90, 50, 20],
                savegText : [156, 90, 50, 50],
                happinessText : [0, 116, 50, 6],
                cornText : [156, 24, 50, 6],
                gdpText : [206, 68, 50, 6],
                slider  : [0, 0, 50, 4],

                
            });


            // load map sprite
            Crafty.sprite(1, "images/map_sprite.png", {
                mapImage : [0, 0, 1280, 270]
            });


            // load game scene sprite
            Crafty.sprite(1, "images/game_components.png", {
                plusButton : [10, 10, 50, 50],
                timelineBackground : [70, 20, 500, 25],
            });





    
        }

    };

}($);
