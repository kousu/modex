1. Download the [OS X Github client](http://mac.github.com/)
2. Open GITHUB and sign in
3. Navigate to [https://github.com/majdal/modex](https://github.com/majdal/modex) and click "Clone in Desktop"
1. Make sure that you have Xcode installed, and up to date, including commandline tools 
4. Download and install "Homebrew" from [here](http://brew.sh/)
5. Download and install Python 2.x.y from [here](http://www.python.org/download/). Choose “Python 2.7.6 Mac OS X 64-bit/32-bit x86-64/i386 Installer".
6. Open `Terminal` from the start menu
7. Create a folder to store your virtual environments (ex. ".virtualenv") and go to that folder
8. Type "sudo easy_install pip" and press Enter
9. Type "sudo easy_install virtualenv==1.10.1" and press Enter
10. Type "virtualenv sig" and press Enter
11. Type "source sig/bin/activate" and press Enter
12. Move back to your cloned "modex" folder
13. Type "pip install -r requirements.txt" and press Enter
14. Type "python run.py" and press Enter. This should open a new browser window pointing to `http://127.0.0.1:8080`.

