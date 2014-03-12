**Date**: January 19th, 2014, 2pm, E3

**Attendees**: n7wilson, kousu, majdal

We're stumped and frozen because we have no stable skeleton of code that does something that can be developed. We need to get this written so that we have a base for the **frontend**, **model** and **model explorer** to get fleshed out from.

**Goals** for the end of the day:

1. Be able to launch a (twisted) server taking as arguments N files (including N=0) containing vector geodata (GDAL should allow us to be agnostic about this) and load them up
2. Pass these geodata layers to the frontend over a websocket and have them rendered overtop of the OpenLayers basemap--the frontend should distinguish between
3. Make sure the frontend *only* loads the geodata for the tile---OpenLayers handles this for rasters properly (by asking a tile server for an [XYZ](Glossary#XYZ) file) but we're not sure if it does something similar with vectors or if we need to implement that ourselves --- this might require emailing the ol3 devs and asking them what their plan is (perhaps we can patch in that functionality for them somehow? requires thinking about the backend protocol though...)
4. Write a hook that implements the 'identify' button: ol3 provides hooks for clicking on vectors on the map, so we want a way to click on them and then ask the backend for what that iesult is and display it somewhere, like on a spare &lt;p&gt; tag sitting below the map. This is our **UI _prototype_**.

1. Provide a d3 canvas on the frontend and a corresponding (over a websocket) feed from the backend giving a stream of numbers (for testing, use something simple like a gaussian or a sin function). This is our **indicator variable _prototype_**.
  * investigate whether it makes sense to multiplex events over the same socket, perhaps with Autobahn

**Extensions** if there's time left over after all that:

1. Nicify the css so that the map takes up 70% of the width and the graphs take up the other 30%, or so that the graphs sit under the map

1. write a skeleton farm model. This probably means reimplementing [FarmSimulationModel](https://github.com/n7wilson/FoodSimulationModel) on top of GDAL. Initially, just write it in the same server as the rest; we'll figure out how to factor out the model from the explorer later ((and ideally the model doesn't have to be in python to work))
1. Investigate (and document in the src/ dir!) what it takes to create raster sources for. Can we get our twisted server to host an http endpoint producing (dynamically, even) [XYZ](Glossary#XYZ) maps?
1. Investigate colouring the different vectors according to some properties. This is our **spatial visualization _prototype_*
1. Investigate pushing update events to the vector layers: e.g. can we get it to display a rainbow wave? This is our **spatial simulation _prototype_**
1. Once we can render arbitrary rasters over to the frontend, figure out what it takes to convince ol3 to invalidate and redownload rasters.
1. Investigate d3.geo: why are/aren't we using it? does it do basemaps? it doesn't do canvases, only svgs, but maybe that's okay?

1. Move the wiki under the repo (so that we can have folders for minutes/ and so that uploading images is easier). Github renders .md files that it finds in repos, so having the wiki there isn't really all that different

### What actually happened
1. kousu showed up late and was confused about where we were meeting :P
1. [Documented](../TechGuides/Step-by-step-installation-instructions-for-Mac-users.md) how to install all the necessary dependencies on OS X 
1. run.py was moved to src/backend/server.py and a slew of related organizational cleanups were done
1. Autobahn gave lots of trouble and we almost tossed it out; the charts are still static
1. Reimplemented Viktor's layer of farm data on top of the basemap, but now merged into the mainline twisted-based workflow
