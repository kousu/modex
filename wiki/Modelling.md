### What do we want to show?

Spatially we want somehow to show production and flows of what is produced (this includes food, money, but also, carbon, nitrogen, and impact on species.)

This is a conversation for later that does not map immediately to the problem of map data..
Ideally we'd also like to show the logic of the model. Eventually I'd like to identify a list of variables users care about (e.g. GDP, farmer income, nitrogen inputs, tax level, food prices) and empirically test the agent model for causal relationships, then show it as a causal map like in Democracy II.(http://www.positech.co.uk/democracy2/ or http://newsgames.gatech.edu/blog/2009/02/newsgame-platforms.html).



### Why use spatial models at all?

It may be useful to think about what maps give us over a non-spatial model.
1. The ability to differentiate costs and quantities of production by properties of the land (wind, soil, rain, temperature etc.) (maybe to look at how climate change and other shifts would change thing)

2. To constrain the model. In particular food production is limited by available land.

3. To initialize the model. The maps as well as stats can data could give us how much wheat/spinach/milk is produced now, and where it comes from.

4. To include spatially specific costs like the financial, carbon, and freshness costs of food transportation. Since the group claims to care a great deal about local food, this matters. (However some of our research suggests that transportation costs are very small compared to other inputs like artificial nitrogen fertilizers and that local food is really a question of jurisdiction and subsidies -- as well as part of a long term strategy to prepare food systems in places like Ontario where we need them because of climate change even though it is slightly more expensive now. Ontario will get warmer, have more water than elsewhere and has relatively high quantities of grade 1 and 2 farmland which is apparently the best..)

5. To look at spatially specific phenomena. E.g. we don't just care about the total number of trees/farms/people/cities but where they are. 

Some examples of the kinds of relationships that might go into a model:
Predators survive if they have a large connected area to roam where there is little risk of getting shot or hit by a car. Pollinators need intact corridors where pesticides are not sprayed. We can even get more specific. Monarch butterflied need milkweed - which is on the noxious weed list so farmers are required to remove it in Ontario. People are happier if they have things like shorter commutes, neighborhoods, with desired properties, and access to open space.
Particular tracts of land have particular historically or ecologically important features. 



### Comments

- Of all of these, the 5th reason is the most challenging and arguably the most interesting/important. We've looked most at the first challenge - how to show production and outputs. This is understandable since the most immediate role of the model is to examine production, flows, costs etc.. The fact that it is an agent based spatial model however means that we can consider richer spatial and network specific phenomena. This could lead to more particular and adaptive policy scenarios.



### Modeling Approach

(Note: We will have to make approximations, since the data is imperfect, but this is the general idea.)

We are making an input output table for each production method for each crop (e.g. organic wheat, conventional wheat, small scale spinach/mixed vegetable, large scale milk production).

For each production method we make a table of all the inputs and outputs.  That is everything produced or consumed in that method. (e.g. seed-, fertilizer-, plows-, corn+, corn husks+, compost+, carbon+, nitrogen+) 

Each has a financial price, but they can also have other costs (for instance time, wellbeing, ecological impact). The costs can be positive or negative (e.g. carbon currently has a price of zero but could have a negative price if there were a carbon tax. Farmers are paid for corn so it has a positive price.)

The input and output quantities can be modified by the properties of the landscape.

Since farmers are assumed (**by who?**) to be price-takers (i.e. agents who are assumed to be too small-scale to affect the market prices individually, as opposed to price-givers who are large and influentials agents that can demand certain pricing margins), we don't need to set prices within the model at least at first. We can take price data from existing food price indices to start. We can modify those prices and under policy interventions, or hypothetical changes in users preferences. 



-------------------

# Coarse Graining of Models

It is typical that a first pass model can be very detailed and very slow, and that in working with it pieces of it are discovered to be simplifiable. We call this _coarse graining_, because a "microscopic" model has "macroscopic" pieces.

A model is "simplifiable" if it can be replaced and still get "the same" outputs (what "the same" is is another huge topic). For example, a random-walk generating weather temperatures could be replaced by a sin function plus some gaussian noise. If the only output of an agent model is some lines ("count of sheep population", "count of wolf population") then it could perhaps be replaced by some ODEs solved numerically (or even symbolically!).

(Modelica)[http://openmodelica.org] has some interesting ideas on plugging submodels together, which we can steal for great justice.
Historically, coarse-graining has been the sugardaddy of western science. Science was all about discovering immutable, eternal laws, like "the earth orbits in an ellipse plus or minus some noise", though now there's more of a recognition that in many systems--especially complex ones--exceptions are the norm.

Along the lines of modelica: [OpenMI](http://openmi.org), which is an API. It's sorttttta like CORBA but for agent and geographical models.

# Parameter Sweeps

Much of our use-case is to explore models which have large numbers of inputs and large number of outputs. The exploration is performed by something called a "parameter sweep", which samples some (but almost never all! definitely not all if any parameters are continuous) points from the input space and runs the models at those points.

* [James Bergstra](http://www.eng.uwaterloo.ca/~jbergstr/publications.html) of the [CTN](http://compneuro.uwaterloo.ca/) has done much work on this.
    * DeepLearning.net<http://DeepLearning.net> Tutorial<http://deeplearning.net/tutorial/> (Yoshua Bengio)
    * UFLDL<http://ufldl.stanford.edu/wiki/index.php/UFLDL_Tutorial> (Andrew Ng)
    * Representative Papers:
        *   J. Bergstra, D. Yamins and D. D. Cox (2013) "Making a Science of Model Search: Hyperparameter Optimization in Hundreds of Dimensions for Vision Architectures<http://jmlr.csail.mit.edu/proceedings/papers/v28/bergstra13.pdf>"
        *   J. Bergstra and Y. Bengio (2012) "Random Search for Hyper-Parameter Optimization<http://www.jmlr.org/papers/volume13/bergstra12a/bergstra12a.pdf>"
        *   J. Bergstra, N. Pinto and D. D. Cox (2012) "Machine Learning for Predictive Auto-Tuning with Boosted Regression Trees<http://www.eng.uwaterloo.ca/~jbergstr/files/pub/12_inpar.pdf>"
        *   J. Bergstra, Y. Bengio and J. Louradour (2011) "Suitability of V1 Energy Models for Object Classification<http://www.mitpressjournals.org/doi/pdf/10.1162/NECO_a_00084>"
        *   A. Courville, J. Bergstra and Y. Bengio (2011) "Unsupervised Models of Images by Spike and Slab RBMs<http://www.icml-2011.org/papers/591_icmlpaper.pdf>"
        *   J. Bergstra, R. Bardenet, Y. Bengio and B. Kégl (2011) "Algorithms for Hyper-parameter Optimization<http://www.eng.uwaterloo.ca/~jbergstr/files/pub/11_nips_hyperopt.pdf>" 
* In [NetLogo](http://ccl.northwestern.edu/netlogo/docs/behaviorspace.htm)
* In [Repast](http://repast.sourceforge.net/docs/RepastParameterSweepsGettingStarted.pdf)

We are also interested in interactively updating parameters. We do not know how to deal with this, yet, except that games deal with this all the time.

### References

(also very disorganized. there are themes here, but they need to be brought out -nick)
http://corridordesign.org/
http://en.wikipedia.org/wiki/Ecosystem_services

### Inspiration

* The [uClassify Collaboratory](http://www.uclassify.com/browse)
* [InsightMaker Collaboratory](http://insightmaker.com/)
* [Statscan's modgen](http://www.statcan.gc.ca/microsimulation/modgen/download-telecharger-eng.htm)
