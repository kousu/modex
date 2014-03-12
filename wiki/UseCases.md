Use Cases and Stories
=====================

## Use Cases

Case: policy makers in a room together with some iPads out, an old Windows XP laptop, and a programmer-researcher presenting them a model to play with.

Case: researchers, especially in the systems and social sciences fields, have complicated models written in ad hoc mixtures of java, R, C, python, etc. they want to look for patterns in them but don't have the time to learn all the math and programming needed to do this. For us to host their model, we allow that the researchers may hire a developer (or become one themselves) enough to retrofit it slightly, but the model's outline (and all of its unit tests, if it has any) must stay identical. All the exploring is done with our statistcal host

Case: sensitivity analysis (ie looking at the derivatives, which, in high-dimensional space, sparsely samples, is really hard)

Case: parameter sweeps

Case: researchers in the world at large, collaborating, github-like.

Case: a running model-explorer instance that can be linked from a news article and invite the general public to come see for themselves

Case: propaganda? (is that evil?)


Our intended use case looks like this:

3. researchers host their model in the Model Explorer (either by asking us with our canonical demo server or by putting an instance of it up themselves)
  * question: does this require more development time, in order to make the frontend be able to understand what features are in the database from the model? it seems difficult to automate things like "make a time slider" without at least telling Model Explorer "this model is time-stepped".
4. researcher invites colleagues to view their model; colleagues open the website and, using a collaborative interface somewhat like jsfiddle or tributary.io, make plots out of the model, ask Model Explorer to compute statistics across whatever dimensions everyone deems interesting; everything 

Case: looking for correlations in high-dimensional data

Case: looking for sensitive parts of statespace

Case: looking for chaotic regions of statespace

Case: looking for path-dependency in complex systems

# Lofty Goals

"encourage policy makers to use data and models in their decisions"

"help people become data literate" ((XXX this requires ourselves to be data literate too, and requires a good dose of [tutorials](Media.md)))

"we want to encourage reproducible science"

"we want people to start imagining alternate worlds to fight T.I.N.A. thinking"

"we want 



------

## Stories

Bib has written some models in NetLogo. He is slowly learning programming concepts like loops, lists and subroutines, but is not totally comfortable yet.
Still, he is trying because he has a strong background in group-dynamics sociology, and feels a strong need to bring computational power to his problems.
He has had advice from a couple people that he should look into Repast, and he's had the odd advice for VB and SciPy.
He knows he wants to have agents, and he knows he wants them to be interacting on GIS data, but he has no idea how to do this.

Alice is a skilled Java programmer and is interested in _helping the world_. She thinks that she could make an impact by getting into computational social science, but she doesn't know where to start. She has never done visualization work in her life.

Terry has a model which generates terabytes of plain text output per second.
Currently, he is analysing this output with a mixture of shell scripts which animate it using igraph and matplotlib and by dumping it into R,
which sometimes crashes and always needs to be babied.
There is no way to fit the output from the complete space of model runs into any computer on Earth.
To get around this, Terry has been sampling model instances sparsely (that is, only (x,y,z) from {1:10}^3 instead of R^3) and even logging data sparsely (not logging data every timestep, but only every 1000th, and when he logs he logs summary statistics instead of the inner state of the model).
Still, a major feature of his work is _model validation_: finding efficient and _**true**_ comparisons betweeen measurements from real brains to measurements from his model brain. 
