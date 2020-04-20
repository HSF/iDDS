
# create packages
python setup.py sdist bdist_wheel

python3 -m twine upload common/dist/idds-common-0.0.3.tar.gz 
python3 -m twine upload client/dist/idds-client-0.0.3.tar.gz
