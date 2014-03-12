# dating.py
# see README.md and `git log` 



## Model Parameters:

GENDER_DISTANCE = 10; # the distance between the means of the gender distributions;
                     #  the gender curves are spaced evenly around 0
                     # see Person for details
FEMALE_SD = 3;       # by default, Females have a wider gender-identity variance than males
MALE_SD   = 1;

# TODO: allow plotting of the expected gender densities from the paramters
#   -- maybe this isn't so important; if modex works, then it should be able to extract them from the data

# TODO: allow characteristics to shift over time, so that dynamics happen

## Model:

import math, random

def sigmoid(x):
    "the logistic function; good for mapping R to (0,1)"
    return 1 / (1 + math.exp(-x))

def squish_spectrum(x):
    "given x on (-Inf,+Inf), compress it to (-1,1)"
    return sigmoid(x)*2 - 1

class Agent(object): pass

class Person(Agent):
    """
    In this version of the model, a person has three characteristics:
      * hotness -- a single (positive) score representing the aggregate of all their positive and negative traits ((obvious flaw in this model: what is attractive to one person is sometimes--often--repulsive to another))
         actually, this is 1 to 10, giving the bro-scale
      * gender  -- a score on -1 to 1 giving the gender-identity (male: -1, 0: androgynous, female: 1) ((objection: Morgan_Holmes would object to this definition))
         In this model, gender is by default generated from a mixture-normal distribution, mixed on sex, with sex chosen with even probability.
         The pure-females distribution has a wider variance than males ((objection: some scholars think gender is independent of sex; this is an open question))
      * orientation -- a score on the same scale as gender giving how attracted
         e.g. a Person with a very male orientation will score highest on Persons who have a high male gender, lowest on those with a high female gender
              a Person with a 0 orientation ignores gender in scoring another Person
         given a chosen sex, orientation is again a smooth bi-modal distribution:
              most people are attracted to the opposite gender with a smaller hump attracted to their own
              first we choose a major orientation, with most people are taken to be attracted to the opposite gender (93%)
              and then we impose a X^2_2 distribution on their actual orientation from there
              (which is actually simpler: first, pick a X^2_2, then align it so that the peak is on the female end if Person's major orientation is to females, and to males otherwise)

         since sex is symmetric, the total orientation distribution should also be bimodal
    """
    def __init__(self, hotness, gender, orientation):
        """
        Construct a Person with the given characteristics.
        If you aren't interested in custom Persons,
          use generate() instead to get the default rules.
        """
        self.hotness = hotness
        #assert -1 <= gender <= 1 #???
        self.gender = gender
        self.orientation = orientation
        
    
    def attraction(self, person):
        "compute the utility function of how attractive person is to self"
        return self.hotness - person.hotness
    
    @staticmethod
    def generate():
        "use the default random distributions to generate an agent"
        hotness = random.choice(range(10))+1 #scale: 1 to 10
        
        sex = random.choice(["male", "female"]) # we do not store sex; this is either a feature or a bug
        gender = random.normalvariate(GENDER_DISTANCE/2*{"male": -1, "female": 1}[sex], {"male": MALE_SD, "female": FEMALE_SD}[sex])
        
        # XXX is this transformation doing what I think it's doing?
        # If I map -Inf..+Inf -> -1 .. 1 via the logistic function,
        # I believe I also squish the probabilities in some funny way
        
        major_orientation = "straight" if random.uniform(0,100) < 93 else "gay"
                      #this number came from Yahoo! Answers, the source of all true wisdom
        #orientation = random.expovariate(.2) #XXX this might need to be 1/.2 because of the two ways of defining Exp(lambda)
        # right now orientation is on (0, +Inf) and bunched up near 0
        # cases:
        # if we are 'male' and straight then our orientation should be to females (ie hovering near the FEMALE_MU)
        #  if we are 'female' and straight then we are to males (ie hovering  near MALE_MU)
        # so
        # XXX can we do something fancier where we look at gender (which is a continous variate)
        #     instead of sex (which is discrete) to decide our orientation?
        #  does that make sense? do androgynes dream of electric sheep and not of either males or females?
        #  ((in my two-sample experience: yes, androgynes tend to prefer other androgynes))
        
        orientation = -gender;

        # at this point, gender is a on a -Inf to +Inf scale,
        gender = squish_spectrum(gender) #to keep this flat
        #orientation = squish_spectrum(orientation)
        
        p = Person(hotness, gender, orientation)
        p._sex = sex
        return p
        
        
## Model Runner:
class World:
    def __init__(self, n=1011):
        self.agents = [Person.generate() for i in range(n)]
    
    def __next__(self):
        " step all the agents "
        while True:
            pass
            yield
    
    def attractions(self):
        "compute the current attractiveness digraph"
        G_A = {} #dicts are slow but whatever
        for i,a in enumerate(self.agents):
            for j,b in enumerate(self.agents):
                G_A[(i,j)] = a.attraction(b)
        return G_A
         
    def iter(self): return self

if __name__ == '__main__':
    import scipy
    import matplotlib.pylab as plt
    
    W = World()
    # extract distributions
    table = scipy.array([(a.hotness, a.gender, a.orientation, {"male": -1, "female": 1}[a._sex]) for a in W.agents])
    plt.hist(table[:,0])
    plt.title("hotness")
    plt.show()
    plt.hist(table[:,1])
    plt.title("gender")
    plt.show()

    plt.hist(table[table[:,3]==-1,][:,1])
    plt.title("gender (sex=m)")
    plt.show()

    plt.hist(table[table[:,3]==1,][:,1])
    plt.title("gender (sex=f)")
    plt.show()

    plt.hist(table[:,2])
    plt.title("orientation")
    plt.show()
