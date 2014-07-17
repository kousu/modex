#!/usr/bin/env Rscript 
# heatmap.R
#a function
# also, a script:
# usage:
# heatmap.R datafile.csv x_variablename y_variablename

heatmap.1d = function(D, xvar, yvar) { #XXX bad name
    # viewing D as a 3d dataset: (x, y, [other cruft])
    # heatmap marginalizes out the cruft by extracting
    # kernel density estimates conditioned on each and every value of x
    # To use this to investigate other conditions--that is, to not marginalize
    # over some variables of your choice--preslice D before you pass it in.
    # BY DESIGN THIS WILL NOT WORK (well) IF X IS A CONTINUOUS VARIABLE
    # TODO: allow changing the density function in use 
    X = D[,xvar] #shorthand
    Y = D[,yvar]
    # go through each possible x variable and..
    R = NULL 
    for(x in unique(X)) {
      # estimate the 1d density function of Y, conditioned on X=x
      # and marginalizing over everything else
      fit = density(Y[X == x], bw=0.2)
      # extract the results and stuff them back in a table
      fit.X = rep(x, length(fit$x))
      fit.Y = fit$x
      fit.density = fit$y 
      
      # pack and store
      block = cbind(fit.X, fit.Y, fit.density)
      R = rbind(R, block) #R sin: constructing datasets by repeated concat! BAD
    }
    R = data.frame(R)
    names(R) = c(xvar, yvar, "density")
    return(R)
}

#TODO:
#heatmap.2d
argv = commandArgs(trailingOnly=T)
if(length(argv)!=3) {
  stop("heatmap needs a dataset and two variables on which to operate")
}
db = argv[1]
act = read.csv(db)
# TODO: support passing by column ID instead of by name
xvar = argv[2]
yvar = argv[3]
z = heatmap.1d(act, xvar, yvar)
#print(z)

#TESTS:
png(filename=paste("scatter_",basename(db),"_",xvar,"_",yvar,".png",sep=""))
plot(act[,xvar], act[,yvar], 
        main="Before heatmapping",
        xlab=xvar, ylab=yvar)

#TODO: use/write remove.file.extension here
png(filename=paste("heatmap_",basename(db),"_",xvar,"_",yvar,".png",sep=""))
colorramp = function(v) { gray(1 - v) }
colorramp = function(v) { hsv(h=0.2, v=v)} #delicious puke green
#colorramp = function(v) { hsv(h=0.7, v=v)} # cyberblue
#colorramp = function(v) { hsv(h=1, v=v)} #cylon red
plot(z[,1], z[,2], col=colorramp(z[,3]/max(z[,3])),
   main="After heatmapping",
   xlab=xvar, ylab=yvar)
   #TODO: a legend
   
   
   #TODO: do some sort of normalization (perhaps with 'approx') to shove all the density results onto the same grid
   # and then we can just use R's built in heatmap() to do this for us
