
Things to try if there are problems:
if attempting to run a custom garlicsim simpack,
and receiving the error
package_name has no attribute __path__, then go to
site-packages/garlicsim/general_misc/import_tools.py
Line 77 fuction add a 'return None' before the rest of the code. This stops import checking.


lalalalalallalalalalalalala