1. Download [Github](http://windows.github.com) from 
2. Open GITHUB and sign in
3. Navigate to [https://github.com/majdal/modex](https://github.com/majdal/modex) and click "Clone in Desktop"
4. Download and install “Visual C++ 2008 Express Edition with SP1”
from: [here](http://www.microsoft.com/visualstudio/en-us/products/2008-editions/express) and  [here](http://www.microsoft.com/en-us/download/confirmation.aspx?id=13276)
5. Download and install Python 2.x.y from [here](http://www.python.org/download/). Choose “Windows Installer” (32bit version) not “Windows X86-64 Installer”.
6. Download and install pywin32 from [here](http://sourceforge.net/projects/pywin32/files/)
  1. Click on `pywin32` folder
  2. Click on the first folder (in this case, Build 217, maybe newer when you try)
  3. Choose the file ending with `.win32-py2.x.exe` -> x being the minor version of Python you installed (in this case, 7) When writing this guide, the file was [this](http://sourceforge.net/projects/pywin32/files/pywin32/Build%20217/pywin32-217.win32-py2.7.exe/download).
7. Open `Github Shell` from the start menu
8. Type `[Environment]::SetEnvironmentVariable("Path", "$env:Path;C:\Python27\;C:\Python27\Scripts\", "User")`
9. This didn't work for me (maybe because of administrator issues) so I had to go to `Control Panel` > search for `Edit the system environment variables`. Once System Properties is open, click on `Environment
Variables` > `New` under System Variables and add Variable Name: `Path`, Variable Value: `$env:Path;C:\Python27\;C:\Python27\Scripts\`
10. Download these two files ([ez_setup](https://bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py), [pip](https://raw.github.com/pypa/pip/master/contrib/get-pip.py))


11. Open `GITHUB Shell`
12. Navigate to where you downloaded the files from Step 10 and run them: First, `python ez_setup.py`, then. `python get-pip.py`
13. Close and open `GITHUB Shell`
14. Navigate to the `modex` folder (Most likely in your `Documents` folder under `GITHUB`)
15. Type `pip install twisted` and press Enter
16. Type `pip install -r requirements.txt` and press `Enter`
17. Type `python run.py` and press Enter
18. Hopefully, this will open a new browser window pointing to `http://127.0.0.1:8080`.

If you have any questions or get stuck, feel free to ask Hala(`h3anwar@uwaterloo.ca`).

May The Odds Be Ever In Your Favor!