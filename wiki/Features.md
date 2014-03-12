Features
========


This is our official Model Explorer feature spec. It is expected to evolve with time. 
See [UseCases](UseCases.md) for the motivation behind all of these
(or, if you can't find it there, challenge the feature in a comment here or write up a case study there)


# Unknowns

Unknowns are up top here so that our warts are out in public for all to see (and hopefully cleverly repair).

How do we support model validation? It seems like that is a very natural and "why didn't you support **that**" [use case](UseCases.md), but it also necessarily involves pulling in non-model data; can

How do we support sparsity?

How do we support branching scenarios (this is what GarlicSIM was supposed to provide)

## V0.01 (due date: March 15th, 2014, the first symposium)

1. A running farm model
  * State:
    1. Farms (polygons)
    2. Farmers (agents)
    3. Distributors (agents)
    4. Tax policy (parameter)
  * Output:
    1. Some estimate of GDP
    2. Carbon?
1. Able to trivially open up a web browser and see the farm model -- basically NetLogo but better
  * so we need data-binds or at leastt o fake them
2. Aesthetically pleasing
3. A single user and a single scenario

## V0.1 (due date: _______ 2014, the first [Social Innovation Lab](http://sig.uwaterloo.ca/feature/social-innovation-labs) of the year)

TODO(kousu): what parts of the V1 do we want done by this?

## V0.2 (due date: end of term April 2014)

TODO(kousu): what parts of the V1 do we want done by this?

having a working demo that we can drop co-ops into quickly is key. If they can't get started they can't help us and then they'll be gone.

## V0.5 (due date: end of term, August 2014)

TODO(kousu): what parts of the V1 do we want done by this?

by this date we need to have something useful. We need to be able to say to people "look, we'll take your model and import it, and then you can poke around at all the graphs".

## V1

What would it take for us to feel that modex was "feature complete"?

1. Still single-user
1. Running a single instance of a model and "watching" it (see NetLogo for great examples of this)
1.  Interacting with a model it as it runs
  * Terry and Nick think that the only way to deal with this is for each model to define an API (eg "move forward", "implement this tax intervention", "drag), and Nick thinks that _modex_ should be agnostic about sending the interventions: if you want to record that an intervention happened you must go onto the server and edit the model to do record it as a piece of data, which then becomes plottable in the usual modex way
1. Able to host **any** (computational) model in **any** programming language, except that the model might need to be modified to output data in a particular way
1. Running many copies of a model in parallel and saving the results all to a single database
  * this feature includes the feature of parameter sweeps, which are super-useful and super-hard
2. Supported Data Types (on both front- and back- end):
  * Tables
    * Datetimes
    * Integers
    * Floats
    * "Factors" (ie Categories) (which is very much different
  * GIS
    * Vectors
    * Rasters
  * Relatively unstructured JSON-like object trees
  * 
1. Automated statistics generation (probably everything that pandas supports we should expose)
  * variances
  * correlations (XXX the standard correlation formula assumes a linear model; is there a generalization? can we simply fit a lot of [GLM](https://duckduckgo.com/Generalized_linear_model)s and take the correlations from the best one?)
  * means
  * medians
  * resampling
    * kernel density estimates
  * stacking/reshaping
  * moving versions of all of these
  * basically everything that pandas supports
1. An in-browser code editor (see: jsfiddle, tributary.io, and the nice 'source' button every example at http://paperjs.org/ has)
1. A way to write an javascript expression which slices a subset of data from the backend
1. Handy, composable, data widgets, which are data-bindable (ie automagically update as new data comes **INCLUDING THE FEATURE SHOWN [HERE](http://square.github.io/crossfilter/): clicking one widget can update the others**):
  * the ability for end-users to compose new visualizations in-browser
  * these widgets need to be pretty
  1. Spreadsheets (ie tables)
  2. Network visualization
    * dynamic
    * hairball(ew) aka force-directed
    * tree-ish
    * ???
  1. Maps
    * vector data
    * raster data
    * objects on the map should be clickable and sensible related information from the other data streams should pop up
  1. Bar charts
    1. Bar charts with confidence and/or credible intervals
  1. Line graphs
    1. Line graphs with confidence and/or credible intervals
  1. Histograms
  1. Scatterplots
  1. Scenario-comparison plots (what this means is hazy at this point, but it definitely involves plotting two datasets on the same axes and might involve something like tweening their colours together in the parts where their "inputs" (whatever those were) were more similar)
1. Data-binds
2. Data-flows
2. Handy, composable, control widgets
  1. Sliders
  2. Number Dials
  3. Map widgets(????)
1. Data export
1. A small but effective suite of **forkable** example visualizations
1. Dataset metadata: explanations of what each column is, change tracking (GIS is big on that)
1. A useful set of unit tests

## V1.5
1. Solid Suite of unit tests
 
## V2
1. (_World Wide_) Web-based Collaboration 
  1. Permalinks for each visualization and model run
  2. Multiuser (so sessions)
  1. (optional) user identities (Facebook connect, Mozilla Persona, OpenID, plus a site-custom account db)
  2. Comments, comments, everywhere
    * on visualizations
    * on models
  2. Forking of explorations (an "exploration" is the set of html5 code that makes the pretty visualizations and chooses what charts to plot)

## V2.5
1. More unit tests
1. a nice way to track exploration forks, like [github's repo graph feature](https://github.com/majdal/modex/network)

## V3

1. Forking of models
  * if we are supporting _any_ model in _any_ language, then in general this is impossible: to fork a model you must fork that codebase. But we can ease the pain if we make setting up the server on your own machine easier and help model writers make their models easier to fork (by reducing or including dependencies)
1. Examples of models to fork, and how to integrate them with Modex
 * agent models (maybe steal a bunch of these from NetLogo)
 * spatially-explicit agent models
 * time-stepped models
 * non-time-stepped models
 * linear models!
 * constant-but-large models!
 * models that output jpgs!
 * models that 
1. (painlessly) Locally runnable:
  1. Windows installer
  2. Mac installer
  3. Clean builds for linux, and attempts to get it into package managers (especially arch and debian)
2. Datasource mashups (so, Stanford should be able to host a repo of websocket-accessible (well, maybe RESTful, and then we wrap that in websockets) data sources 
3. Examples of mashups taking modex-server sources + links at data.gc.ca to do model validation (or to do model invalidation!)

## V3.5
1. More unit tests
