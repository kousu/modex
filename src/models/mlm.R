# mlm.R
# an R script capturing a simple 5-dimensinal multi-linear model:
# y_i = (B1*x_i1 + B2*x_i2 + B3*x_i3 + B4*x_i4 + B5*x_i5) + e_i

# from a command line run with "Rscript mlm.R"
# or load up in your local choice of R environment

# this is useful as a difficult problem to visualize
# while having widely understood math (and tools) underneath it

# it also includes a couple of surprises

f <- function(X) {
  # X should be a data (aka "design") matrix which n rows and 5 columns
  if(is.vector(X)) {
    X = t(X) #coerce to a 1x5 matrix
  }
  n = dim(X)[1]
  d = dim(X)[2]
  stopifnot(d == 5);
  
  B = as.matrix(c(-7.1, -50, 91, -0.00002, pi/3*5)) #the parameters of the model.
                                                         #These should be kept hidden from model explorers.
                                                    # the near-zero term will confuse fitters (like LASSO) that
                                                    # force near-zero terms to 0 under the assumption that they are noise
  
  epsilon = rchisq(n, 2)/2 - 1                         #random, not-quite-gaussian noise
                                                         #(and the answer to life, the universe and eveyrthing)
  y = X%*%B + epsilon
  y
}

#what does it mean to sample a model?

# sample the model:
# 1) choose some points at which to sample
#z = rnorm(101*5, 4, 7)
z = sample(seq(-100, 100, length.out = 101*5))
dim(z) = c(101,5) #cast to a matrix
# 2) evaluate model at those points
y = f(z)
# 3) in this case, R knows how to recover the Bs 
fit = lm(y ~ z)
print(summary(fit))

# case the dataset to a data.frame so that plot(D) will make a nice scatterplot matrix of it
# since B[3] = 91, that column in the scatterplot matrix should show the clearest relationship, in a sea of noise
D = as.data.frame(cbind(y, z))
print(D)
plot(D)
plot(y)
hist(y)

# the question for the future is: what does it mean to explore this model?
