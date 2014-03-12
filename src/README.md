# Model Explorer Developer's Guide

Welcome to Modex! Please excuse the construction dust.

If your copy of modex isn't working, make sure your [dependencies](#dependencies) are up to date. 

## Folder Layout

Each folder should have a README that describes its content in more detail, but as an overview:

* ```assets/``` -- static content, like data
  * ```assets/libs/``` -- external javascript libraries
  * ```assets/images```-- sprites, textures, icons, etc
* ```src/frontend```   -- internal html, javascript, and css 
* ```src/backend```    -- model explorer and the twisted server that feeds it to the frontend
* ```src/models```     -- (_tentative_) different model instances to be hosted by the model explorer
* ```src/scripts```    -- batch scripts, like geoprocessing hacks, one-off webcrawlers, and database cleaners
* ```tests/```         -- unit, integration, acceptance, etc tests
* ```scratch/```       -- demo, testing, and API-learning code goes here. If a demo is at all complicated it should get its own subfolder and include all js libraries it needs locally. Once it cycles through a few iterations its lessons should be merged in ```src/```, but the demo should (within reason) stay put for future reference. 
* ```mockups/```       -- drawings and vision documents

## Dependencies

We depend on several libraries, tools, and sources. The frontend libraries are all in javascript and are either referenced by hyperlink or included under ```assets/```, while the backend libraries require some work on your end. To get hacking on this project, you need to install these dependencies:

* An HTML5-supporting browser (so, any recent Firefox, Chrome, Opera, or even IE)
* [Python 2.x](FIXME)
  * on Linux or OS X you almost certainly already have this; if not, Google how to install it
  * on Windows, go to [download it](http://python.org/download)
* [GDAL](FIXME)
* [GIS Datasets](../assets/maps/README.md) (not all are open; for some you need to be part of [OGDE](www.lio.mnr.gov.on.ca/) to legally obtain them).



You also need several python-specific requirements. They are listed in ```requirements.txt```. **You must put some effort in to decide how you want to install these**. You can either _**install into your system python**_ directory or setup a **_```virtualenv```_** to isolate the packages.

If you want to use ```virtualenv``` first make sure it is installed properly (something like this -- sorry, very much linux specific at the moment; should work on Mac too, but leave a comment if it doesn't)
```
$ sudo pip2 install virtualenv  #make sure virtualenv is install if it isn't already
$ sudo pip2 install virtualenvwrapper #get the scripts that should have come with virtualenv
$ # setup 'workon' to make virtualenvs less tedious
$ cat >> ~/.bashrc <<EOF
export WORKON_HOME=~/.virtualenvs
source /usr/bin/virtualenvwrapper.sh
EOF
$ source ~/.bashrc              #temporarily pull in that change; it will become permanent next login
$ mkdir $WORKON_HOME            #setup a place to put virtualenvs
```

And then construct the environment:
```
$ cd ~/modex           #go to wherever you checked out the repository; it's it's not ~/modex, substitute the correct path
$ mkvirtualenv -p `which python2` modex  #construct the environment; note that we specify python2 here because modex is python2-specific 
$ workon modexenv      #get the missing script that virtualenv should come with
(modexenv)$ which python #you should see the name of the env to the left now, indicating that paths are jailed
/home/yourusername/.virtualenvs/modexenv/bin/python
(modexenv)$ pip install -r requirements.txt
```

Now whenever you want to start working on the project, you must remember the invocation
```
$ cd ~/modex
$ workon modexenv
(modexenv) $
```

**_Virtualenvs_** are an elegant solution to dependency hell, and they allow us to specify with ```requirements.txt``` exactly what code we're desinging against, but they are tedious and take up extra space. Many of the dependencies **_can be isntalled directly into your system_** python ```site-packages/``` folder. Twisted is a great candidate for this: it's extremely stable so there is no need to isolate its version from the mainstream version, and it's large: about 50MB. If you wish to follow this path, then first try installing things with your package manager, and only after you have exhausted that try ```pip```. 

 You can install these dependencies using ```pip``` (which, depending on your system, you might need to run as ```pip2``` since we're developing against Python 2 or):
`pip install -r requirements.txt`

If you don't know if you have a package manager, if you're on linux check if you have ```apt-get``` (aka synaptic), ```pacman```, ```yum```, ```emerge``` or search your distribution's documentation; if you're on OS X see if you have installed [fink](http://fink.thetis.ig42.org/), [port](https://www.macports.org/), or [brew](http://brew.sh/). Then _tediously go through ```requirements.txt``` line by line_ and search in your package manager (and you might need to Google to find the correct package names for your system),

Finally, once that is done, run
```
$ cd ~/modex
$ sudo pip2 install -r requirements.txt
```
to get any leftovers. **You also need to run this when requirements.txt changes**.

Arranging your install this way is more immediately tedious, but it saves you from having to ```workon modexenv``` every day, and saves space on your disk. Do be aware that when you do system updates the updates have a small chance of conflicting with the pip-installed packages, and that if that happens remember you did it yourself.

The choice of which method: virtual or system, is up to you. You will probably have an easier time going with virtualenvs on Windows on OS X, and an easier time with your system package manager on Linux/BSD.


You will probably also want **_these tools_** to make dealing with data easier:
* A spreadsheet. Excel, [LibreOffice Calc](http://www.libreoffice.org/download). You need to be able to work with and debug tabular data. You should be able to do reliable preprocessing of it (sums, averages, subsets) and plotting. (if you are comfortable doing this in matlab, R, or scipy, then by all means stick with what you know).
* A GIS. You can get [ArcGIS](http://esri.com) [from the school](https://uwaterloo.ca/information-systems-technology/services/software-students/microsoft-office-students) at a discount, or you can install [QGIS](http://qgis.org/) which is frankly perhaps better
* Some web developer tools (see [frontend/README.md](frontend/) for suggestions)
* [IPython](http://ipython.org), which gives a python command shell _with tab-completion_, and Mathematica-style [notebooks](http://nbviewer.ipython.org).


For a brain dump of everything you might need to know about getting set up on OS X, see [here](../wiki/TechGuides/Step-by-step-installation-instructions-for-Mac-users.md). Similarly, see [here](../wiki/TechGuides/Step-by-step-installation-instructions-for-Windows-users.md) for Windows.


## Hacking

To get started, open a command prompt and run 'run.py' (which is living a directory up from here, in the project root):
```
[user@laptop modex]$ ./run.py
2014-01-19 22:57:35-0500 [-] Log opened.
2014-01-19 22:57:35-0500 [-] Starting server in /home/kousu/School/WICI/sig/repos/modex
2014-01-19 22:57:35-0500 [-] putting /home/kousu/School/WICI/sig/repos/modex/src/frontend at root
2014-01-19 22:57:35-0500 [-] putting /home/kousu/School/WICI/sig/repos/modex/assets at assets
2014-01-19 22:57:35-0500 [-] Site starting on 8080
2014-01-19 22:57:35-0500 [-] Starting factory <twisted.web.server.Site instance at 0x14cf680>
2014-01-19 22:57:35-0500 [-] Now open http://127.0.0.1:8080 in your browser
```

which should open a browser window pointed at the twisted web application server we're using.
Because of the complicated web-based nature of this project you must always be running the server
while developing, and you need to have a network connection available for at least for the basemaps, and perhaps other sources.

Next, depending on your interest, see [frontend](frontend) or [backend](backend).
Some changes, namely API changes, which are going to be especially common in the early stage,
require working on both sides simultaneously. To do that work, keep both subfolders open and liberally restart the server: press Ctrl-C, wait for it to terminate, and then rerun 'run.py'.

### Gotchas

You can use qgis to fiddle with geodata: subset it, reorder it, remove or add columns, do precomputation, convert formats...
but it's tricky. Some gotchas with qgis:
* SQL joins are hidden under 'properties' of a layer
* to edit a layer it MUST be in ESRI Shapefile format and you need to find the "Toggle Editing" button (which shows up in the rightclick menu on the layer, once its in that format)
